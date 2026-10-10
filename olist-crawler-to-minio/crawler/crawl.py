from __future__ import annotations
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path
from .client import SourceClient
from .config import SETTINGS
from .storage import Storage

MASTER_DATASETS = ["products", "sellers", "customers", "geolocation", "categories"]
TRANSACTION_DATASETS = ["orders", "order_items", "payments", "reviews"]


def date_range(start: str, end: str):
    s, e = date.fromisoformat(start), date.fromisoformat(end)
    if s > e:
        raise ValueError("START_DATE must be <= END_DATE")
    while s <= e:
        yield s.isoformat()
        s += timedelta(days=1)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def crawl_transactions(client: SourceClient, store: Storage, state: dict):
    tx = state.setdefault("transactions", {})
    completed_orders = set(tx.setdefault("order_dates", []))
    completed_reviews = set(tx.setdefault("review_answer_dates", []))

    # Orders define the valid transaction dates. This avoids thousands of empty order requests.
    order_dates = [d for d in client.order_dates() if SETTINGS.start_date <= d <= SETTINGS.end_date]
    for day in order_dates:
        if day in completed_orders:
            print(f"[SKIP] orders/items/payments date={day}")
            continue

        print(f"[CRAWL] orders/items/payments date={day}")
        expected = client.order_count(day) if SETTINGS.verify_order_count else None
        payloads = {
            "orders": client.orders(day),
            "order_items": client.order_items(day),
            "payments": client.payments(day),
        }
        actual_orders = store.csv_row_count(payloads["orders"])
        if expected is not None and actual_orders != expected:
            raise RuntimeError(f"Order validation failed for {day}: API count={expected}, CSV rows={actual_orders}")

        rows = {}
        for dataset, data in payloads.items():
            path = store.write_transaction(dataset, day, data)
            rows[dataset] = {
                "rows": store.csv_row_count(data),
                "sha256": sha256(data),
                "path": str(path).replace("\\", "/"),
            }

        completed_orders.add(day)
        tx["order_dates"] = sorted(completed_orders)
        state.setdefault("_pending_transaction_manifests", {})[day] = {
            "type": "transactional",
            "date": day,
            "source": SETTINGS.source_url,
            "datasets": rows,
            "order_count_api": expected,
        }
        store.save_state(state)
        print(f"[DONE] orders/items/payments date={day}")

    # Reviews are independent of purchase date. A customer may answer a review days/weeks
    # after purchase, so the crawler uses the review_answer_timestamp date stream.
    for day in date_range(SETTINGS.review_start_date, SETTINGS.review_end_date):
        if day in completed_reviews:
            print(f"[SKIP] reviews answer-date={day}")
            continue
        data = client.reviews(day)
        path = store.write_transaction("reviews", day, data)
        info = {
            "type": "transactional",
            "date": day,
            "source": SETTINGS.source_url,
            "datasets": {
                "reviews": {
                    "rows": store.csv_row_count(data),
                    "sha256": sha256(data),
                    "path": str(path).replace("\\", "/"),
                    "date_semantics": "DATE(review_answer_timestamp)",
                }
            },
        }
        store.write_manifest(f"reviews-{day}", info)
        completed_reviews.add(day)
        tx["review_answer_dates"] = sorted(completed_reviews)
        store.save_state(state)
        print(f"[DONE] reviews answer-date={day}")

    # Full-review safeguard: reviews with NULL answer_timestamp are not represented by
    # answer-date partitions. Capture them once through the dedicated API.
    if SETTINGS.crawl_unanswered_reviews and not tx.get("unanswered_reviews_completed", False):
        data = client.unanswered_reviews()
        path = store.write_bytes("reviews/unanswered/reviews_unanswered.csv", data)
        tx["unanswered_reviews_completed"] = True
        tx["unanswered_reviews"] = {
            "rows": store.csv_row_count(data),
            "sha256": sha256(data),
            "path": str(path).replace("\\", "/"),
        }
        store.save_state(state)
        print(f"[DONE] unanswered reviews rows={tx['unanswered_reviews']['rows']}")

    # Emit daily manifests for order/item/payment dates only after all three are present.
    pending = state.get("_pending_transaction_manifests", {})
    for day, manifest in list(pending.items()):
        store.write_manifest(day, manifest)
        del pending[day]
    state.pop("_pending_transaction_manifests", None)
    store.save_state(state)

def bootstrap_master(client: SourceClient, store: Storage, state: dict):
    manifest_response = client.master_manifest()
    manifest = manifest_response.get("datasets", manifest_response)
    for dataset in MASTER_DATASETS:
        info = manifest[dataset]
        target_version = int(info["version"])
        local = state.setdefault("master", {}).get(dataset)
        if local is not None:
            continue

        print(f"[BOOTSTRAP] {dataset} version={target_version}")
        offset = 0
        total = 0
        while True:
            data = client.master_snapshot_page(dataset, SETTINGS.page_size, offset)
            # Empty page = finished. The API returns CSV.
            if not data or store.csv_row_count(data) == 0:
                break
            store.write_snapshot_page(dataset, target_version, offset, data)
            n = store.csv_row_count(data)
            total += n
            offset += SETTINGS.page_size
            if n < SETTINGS.page_size:
                break
        if total != int(info["row_count"]):
            raise RuntimeError(f"Bootstrap validation failed for {dataset}: expected={info['row_count']} downloaded={total}")

        state["master"][dataset] = {
            "version": target_version,
            "etag": info.get("etag"),
            "row_count": total,
        }
        store.save_state(state)
        print(f"[DONE] bootstrap {dataset} rows={total}")


def sync_master(client: SourceClient, store: Storage, state: dict):
    manifest_response = client.master_manifest()
    manifest = manifest_response.get("datasets", manifest_response)
    master_state = state.setdefault("master", {})

    for dataset in MASTER_DATASETS:
        remote = manifest[dataset]
        remote_version = int(remote["version"])
        local = master_state.get(dataset)
        if local is None:
            continue
        local_version = int(local["version"])
        if remote_version == local_version and remote.get("etag") == local.get("etag"):
            print(f"[SKIP] master {dataset} version={local_version}")
            continue
        if remote_version < local_version:
            raise RuntimeError(f"Source version went backwards for {dataset}: local={local_version}, remote={remote_version}")

        print(f"[SYNC] master {dataset}: {local_version} -> {remote_version}")
        current = local_version
        while current < remote_version:
            result = client.master_changes(dataset, current)
            changes = result.get("changes", [])
            if not changes:
                raise RuntimeError(f"No changes returned for {dataset} although version changed {current}->{remote_version}")

            # Changes can contain multiple versions; preserve exactly what source returned.
            max_version = max(int(x["version"]) for x in changes)
            store.write_changes(dataset, max_version, changes)
            current = max_version
            state["master"][dataset] = {
                "version": current,
                "etag": None,
                "row_count": remote.get("row_count"),
            }
            store.save_state(state)

        # Re-read final metadata to capture final ETag.
        final = client.master_metadata(dataset)
        if int(final["version"]) != remote_version:
            raise RuntimeError(f"Master sync race/inconsistency for {dataset}")
        state["master"][dataset] = {
            "version": int(final["version"]),
            "etag": final.get("etag"),
            "row_count": int(final["row_count"]),
        }
        store.save_state(state)
        print(f"[DONE] master {dataset} version={final['version']}")


def main():
    client = SourceClient()
    store = Storage(SETTINGS.raw_dir, SETTINGS.state_file, SETTINGS.manifest_dir)
    state = store.load_state()

    # Master bootstrap happens before incremental sync because the initial state must exist.
    if SETTINGS.run_master and SETTINGS.bootstrap_master:
        bootstrap_master(client, store, state)
    if SETTINGS.run_transactions:
        crawl_transactions(client, store, state)
    if SETTINGS.run_master:
        sync_master(client, store, state)

    print("[FINISHED] Olist crawl completed")

if __name__ == "__main__":
    main()
