from __future__ import annotations

import io
import json
import os
import time
from pathlib import Path
from typing import Any

import pandas as pd
from minio import Minio


class Storage:
    """
    Storage layer:
    - All crawler data is stored directly in MinIO.
    - No raw dataset files are written to local disk.
    - Crawler state is also stored in MinIO.
    """

    def __init__(
        self,
        raw_dir: str,
        state_file: str,
        manifest_dir: str,
    ):
        # These parameters are kept for compatibility with the existing
        # crawler configuration. They are NOT used for local data storage.
        self.raw_dir = raw_dir
        self.state_file = state_file
        self.manifest_dir = manifest_dir

        # ------------------------------------------------------------------
        # MinIO configuration
        # ------------------------------------------------------------------
        self.minio_endpoint = os.getenv(
            "MINIO_ENDPOINT",
            "minio:9000",
        )
        self.minio_access_key = os.getenv(
            "MINIO_ACCESS_KEY",
            "admin",
        )
        self.minio_secret_key = os.getenv(
            "MINIO_SECRET_KEY",
            "minioadmin",
        )
        self.minio_bucket = os.getenv(
            "MINIO_BUCKET",
            "olist",
        )
        self.minio_secure = (
            os.getenv("MINIO_SECURE", "false").lower() == "true"
        )

        self.minio_connect_retries = int(
            os.getenv("MINIO_CONNECT_RETRIES", "30")
        )
        self.minio_connect_delay = float(
            os.getenv("MINIO_CONNECT_DELAY", "2")
        )

        self.minio = Minio(
            self.minio_endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=self.minio_secure,
        )

        self._wait_for_minio()

    # ----------------------------------------------------------------------
    # MinIO connection
    # ----------------------------------------------------------------------

    def _wait_for_minio(self) -> None:
        """Wait until MinIO is reachable and the configured bucket exists."""
        last_error: Exception | None = None

        for attempt in range(1, self.minio_connect_retries + 1):
            try:
                if not self.minio.bucket_exists(self.minio_bucket):
                    raise RuntimeError(
                        f"MinIO bucket '{self.minio_bucket}' does not exist."
                    )

                print(
                    f"[MINIO] Connected to {self.minio_endpoint}, "
                    f"bucket={self.minio_bucket}"
                )
                return

            except Exception as exc:
                last_error = exc

                if attempt < self.minio_connect_retries:
                    print(
                        f"[MINIO] Waiting for MinIO "
                        f"({attempt}/{self.minio_connect_retries})..."
                    )
                    time.sleep(self.minio_connect_delay)

        raise RuntimeError(
            f"Could not connect to MinIO or access bucket "
            f"'{self.minio_bucket}' after "
            f"{self.minio_connect_retries} attempts."
        ) from last_error

    # ----------------------------------------------------------------------
    # Path helpers
    # ----------------------------------------------------------------------

    @staticmethod
    def _normalize_path(path: str) -> str:
        return path.replace("\\", "/").lstrip("/")

    def _data_object_name(self, relative: str) -> str:
        """
        Map logical crawler paths to MinIO objects.

        Example:
            orders/date=2017-09-13/orders.csv

        becomes:

            raw/orders/date=2017-09-13/orders.csv
        """
        relative = self._normalize_path(relative)
        return f"raw/{relative}"

    # ----------------------------------------------------------------------
    # MinIO write
    # ----------------------------------------------------------------------

    def _upload_bytes(
        self,
        object_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> None:
        """Upload bytes directly to MinIO."""
        self.minio.put_object(
            self.minio_bucket,
            object_name,
            io.BytesIO(data),
            length=len(data),
            content_type=content_type,
        )

    # ----------------------------------------------------------------------
    # Crawler state
    # ----------------------------------------------------------------------

    def load_state(self) -> dict[str, Any]:
        """
        Load crawler state directly from MinIO.

        No local state file is created.
        """

        object_name = "raw/_metadata/crawler_state.json"

        try:
            response = self.minio.get_object(
                self.minio_bucket,
                object_name,
            )

            try:
                data = response.read()
            finally:
                response.close()
                response.release_conn()

            return json.loads(data.decode("utf-8"))

        except Exception:
            # State does not exist yet.
            return {
                "transactions": {
                    "completed_dates": []
                },
                "master": {},
            }

    def save_state(self, state: dict[str, Any]) -> None:
        """
        Save crawler state directly to MinIO.

        No temporary local file is created.
        """

        data = json.dumps(
            state,
            indent=2,
            ensure_ascii=False,
        ).encode("utf-8")

        self._upload_bytes(
            "raw/_metadata/crawler_state.json",
            data,
            "application/json",
        )

    # ----------------------------------------------------------------------
    # Generic data writer
    # ----------------------------------------------------------------------

    def write_bytes(
        self,
        relative: str,
        data: bytes,
    ) -> None:
        """
        Store data directly in MinIO.

        No local file is created.
        """

        suffix = Path(relative).suffix.lower()

        content_type = {
            ".csv": "text/csv",
            ".json": "application/json",
            ".jsonl": "application/x-ndjson",
        }.get(
            suffix,
            "application/octet-stream",
        )

        self._upload_bytes(
            self._data_object_name(relative),
            data,
            content_type,
        )

    # ----------------------------------------------------------------------
    # JSON
    # ----------------------------------------------------------------------

    def write_json(
        self,
        relative: str,
        obj: Any,
    ) -> None:
        """Serialize JSON and store it directly in MinIO."""

        data = json.dumps(
            obj,
            indent=2,
            ensure_ascii=False,
        ).encode("utf-8")

        self._upload_bytes(
            self._data_object_name(relative),
            data,
            "application/json",
        )

    # ----------------------------------------------------------------------
    # Master changes
    # ----------------------------------------------------------------------

    def write_changes(
        self,
        dataset: str,
        version: int,
        changes: list[dict[str, Any]],
    ) -> None:
        """
        Store master-data changes directly in MinIO.

        Example:
            raw/products/version=5/changes.jsonl
        """

        rel = (
            f"{dataset}/"
            f"version={version}/"
            f"changes.jsonl"
        )

        data = "".join(
            json.dumps(
                change,
                ensure_ascii=False,
            ) + "\n"
            for change in changes
        ).encode("utf-8")

        self._upload_bytes(
            self._data_object_name(rel),
            data,
            "application/x-ndjson",
        )

    # ----------------------------------------------------------------------
    # Master snapshot
    # ----------------------------------------------------------------------

    def write_snapshot_page(
        self,
        dataset: str,
        version: int,
        offset: int,
        data: bytes,
    ) -> None:
        """
        Store one snapshot page directly in MinIO.

        Example:
            raw/products/version=3/pages/part-0000000000.csv
        """

        relative = (
            f"{dataset}/"
            f"version={version}/"
            f"pages/"
            f"part-{offset:010d}.csv"
        )

        self.write_bytes(
            relative,
            data,
        )

    # ----------------------------------------------------------------------
    # Transaction data
    # ----------------------------------------------------------------------

    def write_transaction(
        self,
        dataset: str,
        day: str,
        data: bytes,
    ) -> None:
        """
        Store daily transaction data directly in MinIO.

        Example:
            raw/orders/date=2017-09-13/orders.csv
        """

        relative = (
            f"{dataset}/"
            f"date={day}/"
            f"{dataset}.csv"
        )

        self.write_bytes(
            relative,
            data,
        )

    # ----------------------------------------------------------------------
    # Manifest
    # ----------------------------------------------------------------------

    def write_manifest(
        self,
        day: str,
        manifest: dict[str, Any],
    ) -> None:
        """
        Store manifest directly in MinIO.

        Example:
            raw/_metadata/manifests/date=2017-09-13.json
        """

        data = json.dumps(
            manifest,
            indent=2,
            ensure_ascii=False,
        ).encode("utf-8")

        self._upload_bytes(
            f"raw/_metadata/manifests/date={day}.json",
            data,
            "application/json",
        )

    # ----------------------------------------------------------------------
    # CSV utilities
    # ----------------------------------------------------------------------

    @staticmethod
    def csv_row_count(data: bytes) -> int:
        """
        Count CSV rows from bytes.

        This does not write the data to disk.
        """

        if not data:
            return 0

        return int(
            len(
                pd.read_csv(
                    io.BytesIO(data)
                )
            )
        )
