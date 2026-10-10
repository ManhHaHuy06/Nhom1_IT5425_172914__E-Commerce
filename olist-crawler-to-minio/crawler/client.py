from __future__ import annotations
import io
import time
from typing import Any
import pandas as pd
import requests
from .config import SETTINGS

class SourceClient:
    def __init__(self):
        self.base = SETTINGS.source_url
        self.session = requests.Session()

    def request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = self.base + path
        last = None
        for attempt in range(1, SETTINGS.max_retries + 1):
            try:
                r = self.session.request(method, url, timeout=SETTINGS.timeout, **kwargs)
                r.raise_for_status()
                return r
            except requests.RequestException as exc:
                last = exc
                if attempt == SETTINGS.max_retries:
                    raise
                time.sleep(SETTINGS.retry_backoff * attempt)
        raise last  # pragma: no cover

    def get_json(self, path: str, **params) -> dict[str, Any]:
        return self.request("GET", path, params=params).json()

    def get_csv_bytes(self, path: str, **params) -> bytes:
        return self.request("GET", path, params=params).content

    # ---------- Group A ----------
    def order_dates(self) -> list[str]:
        return self.get_json("/api/v1/orders/dates")["dates"]

    def order_count(self, day: str) -> int:
        return int(self.get_json(f"/api/v1/orders/date/{day}/count")["count"])

    def orders(self, day: str) -> bytes:
        return self.get_csv_bytes(f"/api/v1/orders/date/{day}")

    def order_items(self, day: str) -> bytes:
        # Preferred endpoint. The fallback keeps compatibility with the older server.
        try:
            return self.get_csv_bytes(f"/api/v1/order-items/order-date/{day}")
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return self.get_csv_bytes(f"/api/v1/order-items/date/{day}")
            raise

    def payments(self, day: str) -> bytes:
        return self.get_csv_bytes(f"/api/v1/payments/order-date/{day}")

    def reviews(self, day: str) -> bytes:
        return self.get_csv_bytes(f"/api/v1/reviews/answer-date/{day}")

    def unanswered_reviews(self, before: str | None = None) -> bytes:
        params = {} if before is None else {"before": before}
        return self.get_csv_bytes("/api/v1/reviews/unanswered", **params)

    # ---------- Group B ----------
    def master_manifest(self) -> dict[str, Any]:
        return self.get_json("/api/v1/master/manifest")

    def master_metadata(self, dataset: str) -> dict[str, Any]:
        return self.get_json(f"/api/v1/{dataset}/metadata")

    def master_snapshot_page(self, dataset: str, limit: int, offset: int) -> bytes:
        return self.get_csv_bytes(f"/api/v1/{dataset}", limit=min(limit, 10000 if dataset == "categories" else 2000000), offset=offset)

    def master_changes(self, dataset: str, since_version: int, limit: int = 100000) -> dict[str, Any]:
        return self.get_json(f"/api/v1/{dataset}/changes", since_version=since_version, limit=limit)
