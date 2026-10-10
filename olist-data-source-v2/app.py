from __future__ import annotations

import hashlib
import io
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

app = FastAPI(
    title="Olist Data Source API",
    description=(
        "Dockerized HTTP source simulator for the Olist Brazilian E-Commerce dataset. "
        "Provides date-based transactional APIs, version/change-based master APIs, "
        "and simulation APIs for testing incremental ingestion."
    ),
    version="2.0.0",
)

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
SIM_DIR = Path(os.getenv("SIMULATION_DIR", "simulation"))
STATE_FILE = SIM_DIR / "master_state.json"

DATASETS = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

MASTER_DATASETS = {
    "products": {
        "file": "olist_products_dataset.csv",
        "key": "product_id",
        "simulation_columns": [
            "product_category_name", "product_name_lenght", "product_description_lenght",
            "product_photos_qty", "product_weight_g", "product_length_cm",
            "product_height_cm", "product_width_cm",
        ],
    },
    "sellers": {
        "file": "olist_sellers_dataset.csv",
        "key": "seller_id",
        "simulation_columns": ["seller_zip_code_prefix", "seller_city", "seller_state"],
    },
    "customers": {
        "file": "olist_customers_dataset.csv",
        "key": "customer_id",
        "simulation_columns": [
            "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"
        ],
    },
    "geolocation": {
        "file": "olist_geolocation_dataset.csv",
        # Raw geolocation has no unique key; API simulation uses a deterministic row key.
        "key": None,
        "simulation_columns": [
            "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
            "geolocation_city", "geolocation_state",
        ],
    },
    "categories": {
        "file": "product_category_name_translation.csv",
        "key": "product_category_name",
        "simulation_columns": ["product_category_name_english"],
    },
}

TRANSACTIONAL_METADATA = {
    "orders": {"type": "transactional", "date_column": "order_purchase_timestamp"},
    "order_items": {"type": "transactional", "date_driver": "orders.order_purchase_timestamp"},
    "payments": {"type": "transactional", "date_driver": "orders.order_purchase_timestamp"},
    "reviews": {"type": "transactional", "date_column": "review_answer_timestamp"},
}


def ensure_sim_dir() -> None:
    SIM_DIR.mkdir(parents=True, exist_ok=True)


def dataset_path(name: str) -> Path:
    if name == "payments":
        name = "order_payments"
    elif name == "reviews":
        name = "order_reviews"
    if name not in DATASETS:
        raise HTTPException(status_code=404, detail=f"Unknown dataset: {name}")
    path = DATA_DIR / DATASETS[name]
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Dataset file not found: {path}")
    return path


def parse_day(value: str) -> pd.Timestamp:
    try:
        ts = pd.Timestamp(value)
        if pd.isna(ts):
            raise ValueError
        return ts.normalize()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid date. Use YYYY-MM-DD") from exc


def csv_response(df: pd.DataFrame, filename: str) -> StreamingResponse:
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def json_safe(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def row_to_dict(row: pd.Series) -> dict[str, Any]:
    return {str(k): json_safe(v) for k, v in row.items()}


def load_state() -> dict[str, dict[str, Any]]:
    ensure_sim_dir()
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))

    state: dict[str, dict[str, Any]] = {}
    for name, cfg in MASTER_DATASETS.items():
        path = DATA_DIR / cfg["file"]
        if not path.exists():
            continue
        row_count = int(sum(1 for _ in path.open("r", encoding="utf-8-sig", newline="")) - 1)
        state[name] = {
            "version": 1,
            "row_count": max(row_count, 0),
            "change_count": 0,
            "last_changed": None,
        }
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    return state


STATE = load_state()


def save_state() -> None:
    ensure_sim_dir()
    STATE_FILE.write_text(json.dumps(STATE, indent=2), encoding="utf-8")


def changes_file(dataset: str) -> Path:
    ensure_sim_dir()
    return SIM_DIR / f"{dataset}_changes.jsonl"


def append_change(dataset: str, change_type: str, key: Any, data: dict[str, Any] | None) -> dict[str, Any]:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    entry = {
        "version": int(STATE[dataset]["version"] + 1),
        "timestamp": now,
        "change_type": change_type,
        "key": key,
        "data": data,
    }
    with changes_file(dataset).open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    STATE[dataset]["version"] = entry["version"]
    STATE[dataset]["change_count"] += 1
    STATE[dataset]["last_changed"] = now
    save_state()
    return entry


def read_changes(dataset: str, since_version: int = 0, limit: int = 10000) -> list[dict[str, Any]]:
    path = changes_file(dataset)
    if not path.exists():
        return []
    result = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            entry = json.loads(line)
            if int(entry["version"]) > since_version:
                result.append(entry)
                if len(result) >= limit:
                    break
    return result


def etag_for(dataset: str) -> str:
    s = json.dumps(STATE[dataset], sort_keys=True)
    base = DATA_DIR / MASTER_DATASETS[dataset]["file"]
    payload = f"{base.name}:{base.stat().st_size}:{base.stat().st_mtime_ns}:{s}"
    return hashlib.sha256(payload.encode()).hexdigest()


def master_df(dataset: str, include_internal: bool = False) -> pd.DataFrame:
    cfg = MASTER_DATASETS[dataset]
    df = pd.read_csv(DATA_DIR / cfg["file"])
    changes = read_changes(dataset, since_version=0, limit=10_000_000)

    key = cfg["key"]
    if dataset == "geolocation":
        # Raw geolocation has no unique source key. Give every base row a stable
        # synthetic key and every simulated row a sim-* key.
        df["__source_row_key"] = [f"base-{i:08d}" for i in range(len(df))]
        for ch in changes:
            if ch["change_type"] == "INSERT":
                row = pd.DataFrame([ch["data"]])
                row["__source_row_key"] = ch["key"]
                df = pd.concat([df, row], ignore_index=True)
            elif ch["change_type"] in ("UPDATE", "DELETE"):
                idx = df.index[df["__source_row_key"] == ch["key"]]
                if len(idx) == 0:
                    continue
                if ch["change_type"] == "DELETE":
                    df = df.drop(index=idx)
                else:
                    for col, val in ch["data"].items():
                        if col in df.columns:
                            df.loc[idx, col] = val
        if not include_internal:
            df = df.drop(columns=["__source_row_key"], errors="ignore")
        return df.reset_index(drop=True)

    if key and changes:
        df["__key"] = df[key].astype(str)
        for ch in changes:
            k = str(ch["key"])
            idx = df.index[df["__key"] == k]
            if ch["change_type"] == "INSERT":
                row = pd.DataFrame([ch["data"]])
                df = pd.concat([df, row], ignore_index=True)
                df["__key"] = df[key].astype(str)
            elif ch["change_type"] == "UPDATE":
                if len(idx):
                    for col, val in ch["data"].items():
                        if col in df.columns:
                            df.loc[idx[0], col] = val
            elif ch["change_type"] == "DELETE":
                if len(idx):
                    df = df.drop(index=idx[0])
        df = df.drop(columns=["__key"], errors="ignore")
    return df.reset_index(drop=True)


def geolocation_row_key(row: pd.Series) -> str:
    # Retained for compatibility with helper code; current base-row keys are positional.
    raw = "|".join("" if pd.isna(row.get(c)) else str(row.get(c)) for c in MASTER_DATASETS["geolocation"]["simulation_columns"])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def master_metadata(dataset: str) -> dict[str, Any]:
    st = STATE[dataset]
    return {
        "dataset": dataset,
        "type": "master",
        "version": st["version"],
        "row_count": st["row_count"],
        "etag": etag_for(dataset),
        "last_changed": st["last_changed"],
        "change_count": st["change_count"],
        "change_endpoint": f"/api/v1/{dataset}/changes?since_version={{version}}",
    }


@app.get("/", tags=["system"])
def root():
    return {
        "service": "olist-data-source",
        "version": app.version,
        "status": "ok",
        "docs": "/docs",
        "api_prefix": "/api/v1",
    }


@app.get("/health", tags=["system"])
def health():
    missing = [name for name, file in DATASETS.items() if not (DATA_DIR / file).exists()]
    return {
        "status": "ok" if not missing else "degraded",
        "data_dir": str(DATA_DIR),
        "simulation_dir": str(SIM_DIR),
        "missing": missing,
    }


@app.get("/api/v1/info", tags=["system"])
def info():
    return {
        "service": "Olist Data Source API",
        "api_version": "v1",
        "server_version": app.version,
        "groups": {
            "transactional": ["orders", "order_items", "payments", "reviews"],
            "master": ["products", "sellers", "customers", "geolocation", "categories"],
        },
        "strategies": {
            "transactional": "date-based incremental extraction",
            "master": "version/change-based incremental synchronization",
        },
    }


@app.get("/api/v1/datasets", tags=["datasets"])
def list_datasets():
    result = []
    for name, filename in DATASETS.items():
        path = DATA_DIR / filename
        public_name = {"order_payments": "payments", "order_reviews": "reviews"}.get(name, name)
        dtype = "master" if public_name in MASTER_DATASETS else "transactional"
        result.append({
            "name": public_name,
            "filename": filename,
            "type": dtype,
            "available": path.exists(),
            "size_bytes": path.stat().st_size if path.exists() else None,
        })
    return {"count": len(result), "datasets": result}


@app.get("/api/v1/datasets/{name}/metadata", tags=["datasets"])
def generic_metadata(name: str):
    if name in MASTER_DATASETS:
        return master_metadata(name)
    if name == "payments":
        name = "order_payments"
    if name == "reviews":
        name = "order_reviews"
    path = dataset_path(name)
    return {
        "dataset": name,
        "type": TRANSACTIONAL_METADATA.get(name, {}).get("type", "dataset"),
        "filename": path.name,
        "size_bytes": path.stat().st_size,
        "date_column": TRANSACTIONAL_METADATA.get(name, {}).get("date_column"),
        "date_driver": TRANSACTIONAL_METADATA.get(name, {}).get("date_driver"),
    }


@app.get("/api/v1/datasets/{name}/download", tags=["datasets"])
def download_dataset(name: str):
    path = dataset_path(name)
    return FileResponse(path, media_type="text/csv", filename=path.name)


@app.get("/api/v1/datasets/{name}/sample", tags=["datasets"])
def sample_dataset(name: str, limit: int = Query(100, ge=1, le=10000)):
    path = dataset_path(name)
    df = pd.read_csv(path, nrows=limit)
    return {"dataset": name, "count": len(df), "data": df.where(pd.notna(df), None).to_dict(orient="records")}


# ----------------------------- Group A: Orders -----------------------------

ORDERS_FILE = DATA_DIR / DATASETS["orders"]
ORDER_ITEMS_FILE = DATA_DIR / DATASETS["order_items"]
PAYMENTS_FILE = DATA_DIR / DATASETS["order_payments"]
REVIEWS_FILE = DATA_DIR / DATASETS["order_reviews"]


@app.get("/api/v1/orders/dates", tags=["orders"])
def order_dates():
    df = pd.read_csv(ORDERS_FILE, usecols=["order_purchase_timestamp"])
    dates = pd.to_datetime(df["order_purchase_timestamp"], errors="coerce").dt.strftime("%Y-%m-%d").dropna().drop_duplicates().sort_values().tolist()
    return {
        "dataset": "orders",
        "date_column": "order_purchase_timestamp",
        "min_date": dates[0] if dates else None,
        "max_date": dates[-1] if dates else None,
        "count": len(dates),
        "dates": dates,
    }


@app.get("/api/v1/orders/date/{day}", tags=["orders"])
def orders_by_date(day: str):
    target = parse_day(day)
    df = pd.read_csv(ORDERS_FILE)
    ts = pd.to_datetime(df["order_purchase_timestamp"], errors="coerce")
    return csv_response(df.loc[ts.dt.normalize() == target].copy(), f"orders_{day}.csv")


@app.get("/api/v1/orders/date/{day}/count", tags=["orders"])
def order_count_by_date(day: str):
    target = parse_day(day)
    df = pd.read_csv(ORDERS_FILE, usecols=["order_purchase_timestamp"])
    ts = pd.to_datetime(df["order_purchase_timestamp"], errors="coerce")
    return {"date": day, "dataset": "orders", "count": int((ts.dt.normalize() == target).sum())}


@app.get("/api/v1/orders/{order_id}", tags=["orders"])
def order_by_id(order_id: str):
    df = pd.read_csv(ORDERS_FILE)
    result = df[df["order_id"].astype(str) == order_id]
    if result.empty:
        raise HTTPException(status_code=404, detail="Order not found")
    return row_to_dict(result.iloc[0])


# -------------------------- Group A: Order Items ---------------------------

@app.get("/api/v1/order-items/order-date/{day}", tags=["order-items"])
def order_items_by_order_date(day: str):
    target = parse_day(day)
    orders = pd.read_csv(ORDERS_FILE, usecols=["order_id", "order_purchase_timestamp"])
    ts = pd.to_datetime(orders["order_purchase_timestamp"], errors="coerce")
    order_ids = set(orders.loc[ts.dt.normalize() == target, "order_id"].dropna())
    items = pd.read_csv(ORDER_ITEMS_FILE)
    return csv_response(items[items["order_id"].isin(order_ids)].copy(), f"order_items_{day}.csv")


# Backward-compatible endpoint used by the earlier crawler.
@app.get("/api/v1/order-items/date/{day}", tags=["order-items"])
def order_items_by_date_compat(day: str):
    return order_items_by_order_date(day)


@app.get("/api/v1/orders/{order_id}/items", tags=["order-items"])
def order_items_by_order(order_id: str):
    df = pd.read_csv(ORDER_ITEMS_FILE)
    return csv_response(df[df["order_id"].astype(str) == order_id].copy(), f"order_items_{order_id}.csv")


# ----------------------------- Group A: Payments ---------------------------

@app.get("/api/v1/payments/order-date/{day}", tags=["payments"])
def payments_by_order_date(day: str):
    target = parse_day(day)
    orders = pd.read_csv(ORDERS_FILE, usecols=["order_id", "order_purchase_timestamp"])
    ts = pd.to_datetime(orders["order_purchase_timestamp"], errors="coerce")
    order_ids = set(orders.loc[ts.dt.normalize() == target, "order_id"].dropna())
    payments = pd.read_csv(PAYMENTS_FILE)
    return csv_response(payments[payments["order_id"].isin(order_ids)].copy(), f"payments_{day}.csv")


@app.get("/api/v1/orders/{order_id}/payments", tags=["payments"])
def payments_by_order(order_id: str):
    df = pd.read_csv(PAYMENTS_FILE)
    return csv_response(df[df["order_id"].astype(str) == order_id].copy(), f"payments_{order_id}.csv")


# ----------------------------- Group A: Reviews ----------------------------

@app.get("/api/v1/reviews/answer-date/{day}", tags=["reviews"])
def reviews_by_answer_date(day: str):
    target = parse_day(day)
    df = pd.read_csv(REVIEWS_FILE)
    ts = pd.to_datetime(df["review_answer_timestamp"], errors="coerce")
    return csv_response(df.loc[ts.dt.normalize() == target].copy(), f"reviews_{day}.csv")


@app.get("/api/v1/reviews/creation-date/{day}", tags=["reviews"])
def reviews_by_creation_date(day: str):
    target = parse_day(day)
    df = pd.read_csv(REVIEWS_FILE)
    ts = pd.to_datetime(df["review_creation_date"], errors="coerce")
    return csv_response(df.loc[ts.dt.normalize() == target].copy(), f"reviews_creation_{day}.csv")


@app.get("/api/v1/reviews/unanswered", tags=["reviews"])
def unanswered_reviews(before: str | None = Query(None)):
    df = pd.read_csv(REVIEWS_FILE)
    mask = pd.to_datetime(df["review_answer_timestamp"], errors="coerce").isna()
    result = df.loc[mask].copy()
    if before:
        cutoff = parse_day(before)
        created = pd.to_datetime(result["review_creation_date"], errors="coerce")
        result = result.loc[created.dt.normalize() < cutoff]
    return csv_response(result, "reviews_unanswered.csv")


@app.get("/api/v1/orders/{order_id}/reviews", tags=["reviews"])
def reviews_by_order(order_id: str):
    df = pd.read_csv(REVIEWS_FILE)
    return csv_response(df[df["order_id"].astype(str) == order_id].copy(), f"reviews_{order_id}.csv")


# ----------------------------- Group B: Master -----------------------------

@app.get("/api/v1/master/manifest", tags=["master"])
def master_manifest():
    return {
        "datasets": {name: master_metadata(name) for name in MASTER_DATASETS}
    }


def master_collection(dataset: str, limit: int, offset: int) -> pd.DataFrame:
    df = master_df(dataset)
    return df.iloc[offset:offset + limit].copy()


@app.get("/api/v1/products", tags=["products"])
def get_products(limit: int = Query(2000000, ge=1, le=2000000), offset: int = Query(0, ge=0)):
    return csv_response(master_collection("products", limit, offset), f"products_{offset}_{limit}.csv")


@app.get("/api/v1/products/metadata", tags=["products"])
def products_metadata():
    return master_metadata("products")


@app.get("/api/v1/products/changes", tags=["products"])
def products_changes(since_version: int = Query(0, ge=0), limit: int = Query(10000, ge=1, le=100000)):
    return {"dataset": "products", "changes": read_changes("products", since_version, limit), "current_version": STATE["products"]["version"]}


@app.get("/api/v1/products/{product_id}", tags=["products"])
def product_by_id(product_id: str):
    df = master_df("products")
    result = df[df["product_id"].astype(str) == product_id]
    if result.empty:
        raise HTTPException(status_code=404, detail="Product not found")
    return row_to_dict(result.iloc[0])


@app.get("/api/v1/sellers", tags=["sellers"])
def get_sellers(limit: int = Query(2000000, ge=1, le=2000000), offset: int = Query(0, ge=0)):
    return csv_response(master_collection("sellers", limit, offset), f"sellers_{offset}_{limit}.csv")


@app.get("/api/v1/sellers/metadata", tags=["sellers"])
def sellers_metadata():
    return master_metadata("sellers")


@app.get("/api/v1/sellers/changes", tags=["sellers"])
def sellers_changes(since_version: int = Query(0, ge=0), limit: int = Query(10000, ge=1, le=100000)):
    return {"dataset": "sellers", "changes": read_changes("sellers", since_version, limit), "current_version": STATE["sellers"]["version"]}


@app.get("/api/v1/sellers/{seller_id}", tags=["sellers"])
def seller_by_id(seller_id: str):
    df = master_df("sellers")
    result = df[df["seller_id"].astype(str) == seller_id]
    if result.empty:
        raise HTTPException(status_code=404, detail="Seller not found")
    return row_to_dict(result.iloc[0])


@app.get("/api/v1/customers", tags=["customers"])
def get_customers(limit: int = Query(2000000, ge=1, le=2000000), offset: int = Query(0, ge=0)):
    return csv_response(master_collection("customers", limit, offset), f"customers_{offset}_{limit}.csv")


@app.get("/api/v1/customers/metadata", tags=["customers"])
def customers_metadata():
    return master_metadata("customers")


@app.get("/api/v1/customers/changes", tags=["customers"])
def customers_changes(since_version: int = Query(0, ge=0), limit: int = Query(10000, ge=1, le=100000)):
    return {"dataset": "customers", "changes": read_changes("customers", since_version, limit), "current_version": STATE["customers"]["version"]}


@app.get("/api/v1/customers/{customer_id}", tags=["customers"])
def customer_by_id(customer_id: str):
    df = master_df("customers")
    result = df[df["customer_id"].astype(str) == customer_id]
    if result.empty:
        raise HTTPException(status_code=404, detail="Customer not found")
    return row_to_dict(result.iloc[0])


@app.get("/api/v1/customers/unique/{customer_unique_id}", tags=["customers"])
def customers_by_unique_id(customer_unique_id: str):
    df = master_df("customers")
    result = df[df["customer_unique_id"].astype(str) == customer_unique_id].copy()
    return csv_response(result, f"customers_unique_{customer_unique_id}.csv")


@app.get("/api/v1/geolocation", tags=["geolocation"])
def get_geolocation(limit: int = Query(2000000, ge=1, le=2000000), offset: int = Query(0, ge=0)):
    return csv_response(master_collection("geolocation", limit, offset), f"geolocation_{offset}_{limit}.csv")


@app.get("/api/v1/geolocation/metadata", tags=["geolocation"])
def geolocation_metadata():
    return master_metadata("geolocation")


@app.get("/api/v1/geolocation/changes", tags=["geolocation"])
def geolocation_changes(since_version: int = Query(0, ge=0), limit: int = Query(10000, ge=1, le=100000)):
    return {"dataset": "geolocation", "changes": read_changes("geolocation", since_version, limit), "current_version": STATE["geolocation"]["version"]}


@app.get("/api/v1/geolocation/{zip_code_prefix}", tags=["geolocation"])
def geolocation_by_zip(zip_code_prefix: str):
    df = master_df("geolocation")
    result = df[df["geolocation_zip_code_prefix"].astype(str).str.zfill(5) == zip_code_prefix.zfill(5)]
    return csv_response(result, f"geolocation_{zip_code_prefix}.csv")


@app.get("/api/v1/categories", tags=["categories"])
def get_categories(limit: int = Query(1000, ge=1, le=10000), offset: int = Query(0, ge=0)):
    return csv_response(master_collection("categories", limit, offset), f"categories_{offset}_{limit}.csv")


@app.get("/api/v1/categories/metadata", tags=["categories"])
def categories_metadata():
    return master_metadata("categories")


@app.get("/api/v1/categories/changes", tags=["categories"])
def categories_changes(since_version: int = Query(0, ge=0), limit: int = Query(10000, ge=1, le=100000)):
    return {"dataset": "categories", "changes": read_changes("categories", since_version, limit), "current_version": STATE["categories"]["version"]}


@app.get("/api/v1/categories/{category_name}", tags=["categories"])
def category_by_name(category_name: str):
    df = master_df("categories")
    result = df[df["product_category_name"].fillna("").astype(str) == category_name]
    if result.empty:
        raise HTTPException(status_code=404, detail="Category not found")
    return row_to_dict(result.iloc[0])


# -------------------------- Simulation API models --------------------------

class AddRecord(BaseModel):
    data: dict[str, Any]


class UpdateRecord(BaseModel):
    key: str
    changes: dict[str, Any]


class DeleteRecord(BaseModel):
    key: str


class GeoRecord(BaseModel):
    data: dict[str, Any]


class GeoUpdate(BaseModel):
    row_key: str
    changes: dict[str, Any]


class GeoDelete(BaseModel):
    row_key: str


def validate_columns(dataset: str, data: dict[str, Any], require_all: bool = False) -> None:
    allowed = set(MASTER_DATASETS[dataset]["simulation_columns"])
    key = MASTER_DATASETS[dataset]["key"]
    if key:
        allowed.add(key)
    unknown = set(data) - allowed
    if unknown:
        raise HTTPException(status_code=400, detail=f"Unknown columns: {sorted(unknown)}")
    if require_all:
        missing = allowed - set(data)
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing columns: {sorted(missing)}")


def simulate_add(dataset: str, data: dict[str, Any]) -> dict[str, Any]:
    cfg = MASTER_DATASETS[dataset]
    validate_columns(dataset, data)
    current = master_df(dataset)

    if dataset == "geolocation":
        key = f"sim-{uuid.uuid4()}"
        payload = {col: data.get(col) for col in cfg["simulation_columns"]}
        entry = append_change(dataset, "INSERT", key, payload)
        STATE[dataset]["row_count"] += 1
        save_state()
        return {"status": "created", "dataset": dataset, "key": key, "change": entry}

    key = cfg["key"]
    supplied_key = data.get(key)
    if supplied_key is None or str(supplied_key).strip() == "":
        supplied_key = str(uuid.uuid4())
        data[key] = supplied_key
    if str(supplied_key) in set(current[key].astype(str)):
        raise HTTPException(status_code=409, detail=f"{key} already exists: {supplied_key}")
    payload = {col: data.get(col) for col in [key] + cfg["simulation_columns"]}
    entry = append_change(dataset, "INSERT", str(supplied_key), payload)
    STATE[dataset]["row_count"] += 1
    save_state()
    return {"status": "created", "dataset": dataset, "key": str(supplied_key), "change": entry}


def simulate_update(dataset: str, key_value: str, changes: dict[str, Any]) -> dict[str, Any]:
    cfg = MASTER_DATASETS[dataset]
    validate_columns(dataset, changes)
    if cfg["key"] is None:
        raise HTTPException(status_code=400, detail="Geolocation uses row_key; use the geolocation-specific simulation endpoint")
    current = master_df(dataset)
    key = cfg["key"]
    matches = current[current[key].astype(str) == str(key_value)]
    if matches.empty:
        raise HTTPException(status_code=404, detail=f"Record not found: {key_value}")
    updated = row_to_dict(matches.iloc[0])
    for col, val in changes.items():
        updated[col] = val
    # Store the complete current row so downstream incremental ingestion has the full new state.
    entry = append_change(dataset, "UPDATE", str(key_value), updated)
    return {"status": "updated", "dataset": dataset, "key": str(key_value), "change": entry}


def simulate_delete(dataset: str, key_value: str) -> dict[str, Any]:
    cfg = MASTER_DATASETS[dataset]
    if cfg["key"] is None:
        raise HTTPException(status_code=400, detail="Geolocation uses row_key; use the geolocation-specific simulation endpoint")
    current = master_df(dataset)
    key = cfg["key"]
    matches = current[current[key].astype(str) == str(key_value)]
    if matches.empty:
        raise HTTPException(status_code=404, detail=f"Record not found: {key_value}")
    old = row_to_dict(matches.iloc[0])
    entry = append_change(dataset, "DELETE", str(key_value), old)
    STATE[dataset]["row_count"] -= 1
    save_state()
    return {"status": "deleted", "dataset": dataset, "key": str(key_value), "change": entry}


# Product / Seller / Customer / Category simulation
for _dataset, _prefix in [
    ("products", "products"),
    ("sellers", "sellers"),
    ("customers", "customers"),
    ("categories", "categories"),
]:
    # Explicit routes are defined below instead of dynamic decorators so Swagger is clear.
    pass


@app.post("/api/v1/simulation/products/add", tags=["simulation"])
def simulation_products_add(body: AddRecord):
    return simulate_add("products", body.data)


@app.post("/api/v1/simulation/products/update", tags=["simulation"])
def simulation_products_update(body: UpdateRecord):
    return simulate_update("products", body.key, body.changes)


@app.post("/api/v1/simulation/products/delete", tags=["simulation"])
def simulation_products_delete(body: DeleteRecord):
    return simulate_delete("products", body.key)


@app.post("/api/v1/simulation/sellers/add", tags=["simulation"])
def simulation_sellers_add(body: AddRecord):
    return simulate_add("sellers", body.data)


@app.post("/api/v1/simulation/sellers/update", tags=["simulation"])
def simulation_sellers_update(body: UpdateRecord):
    return simulate_update("sellers", body.key, body.changes)


@app.post("/api/v1/simulation/sellers/delete", tags=["simulation"])
def simulation_sellers_delete(body: DeleteRecord):
    return simulate_delete("sellers", body.key)


@app.post("/api/v1/simulation/customers/add", tags=["simulation"])
def simulation_customers_add(body: AddRecord):
    return simulate_add("customers", body.data)


@app.post("/api/v1/simulation/customers/update", tags=["simulation"])
def simulation_customers_update(body: UpdateRecord):
    return simulate_update("customers", body.key, body.changes)


@app.post("/api/v1/simulation/customers/delete", tags=["simulation"])
def simulation_customers_delete(body: DeleteRecord):
    return simulate_delete("customers", body.key)


@app.post("/api/v1/simulation/categories/add", tags=["simulation"])
def simulation_categories_add(body: AddRecord):
    return simulate_add("categories", body.data)


@app.post("/api/v1/simulation/categories/update", tags=["simulation"])
def simulation_categories_update(body: UpdateRecord):
    return simulate_update("categories", body.key, body.changes)


@app.post("/api/v1/simulation/categories/delete", tags=["simulation"])
def simulation_categories_delete(body: DeleteRecord):
    return simulate_delete("categories", body.key)


@app.post("/api/v1/simulation/geolocation/add", tags=["simulation"])
def simulation_geolocation_add(body: GeoRecord):
    return simulate_add("geolocation", body.data)


@app.post("/api/v1/simulation/geolocation/update", tags=["simulation"])
def simulation_geolocation_update(body: GeoUpdate):
    validate_columns("geolocation", body.changes)
    current = master_df("geolocation", include_internal=True)
    matches = current[current["__source_row_key"] == body.row_key]
    if matches.empty:
        raise HTTPException(status_code=404, detail=f"Geolocation row not found: {body.row_key}")
    updated = row_to_dict(matches.iloc[0].drop(labels=["__source_row_key"]))
    updated.update(body.changes)
    entry = append_change("geolocation", "UPDATE", body.row_key, updated)
    return {"status": "updated", "dataset": "geolocation", "row_key": body.row_key, "change": entry}


@app.post("/api/v1/simulation/geolocation/delete", tags=["simulation"])
def simulation_geolocation_delete(body: GeoDelete):
    current = master_df("geolocation", include_internal=True)
    matches = current[current["__source_row_key"] == body.row_key]
    if matches.empty:
        raise HTTPException(status_code=404, detail=f"Geolocation row not found: {body.row_key}")
    old = row_to_dict(matches.iloc[0].drop(labels=["__source_row_key"]))
    entry = append_change("geolocation", "DELETE", body.row_key, old)
    STATE["geolocation"]["row_count"] -= 1
    save_state()
    return {"status": "deleted", "dataset": "geolocation", "row_key": body.row_key, "change": entry}


@app.post("/api/v1/simulation/reset/{dataset}", tags=["simulation"])
def simulation_reset(dataset: str):
    if dataset not in MASTER_DATASETS:
        raise HTTPException(status_code=404, detail=f"Unknown master dataset: {dataset}")
    path = changes_file(dataset)
    if path.exists():
        path.unlink()
    base = DATA_DIR / MASTER_DATASETS[dataset]["file"]
    row_count = int(sum(1 for _ in base.open("r", encoding="utf-8-sig", newline="")) - 1)
    STATE[dataset] = {"version": 1, "row_count": max(row_count, 0), "change_count": 0, "last_changed": None}
    save_state()
    return {"status": "reset", "dataset": dataset, "metadata": master_metadata(dataset)}
