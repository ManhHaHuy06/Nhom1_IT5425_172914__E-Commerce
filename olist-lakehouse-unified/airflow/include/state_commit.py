"""Pure, testable guard for Airflow ingestion-state commits."""
from __future__ import annotations


def build_ingestion_state_updates(
    plan: dict,
    tx_loaded: dict,
    ref_loaded: dict,
    reference_extract: dict | None,
    failed_tasks: list[str],
    clickhouse_succeeded: bool,
    success_at: str,
) -> dict[str, str]:
    """Build Variable updates only after all required downstream work succeeded."""
    if failed_tasks:
        raise ValueError(
            "Pipeline did not complete; refusing to commit ingestion state. "
            f"Failed tasks: {failed_tasks}"
        )
    if not isinstance(tx_loaded, dict) or not isinstance(ref_loaded, dict):
        raise ValueError("Missing transaction/reference load results; refusing state commit")
    if "rows" not in tx_loaded or "rows" not in ref_loaded or "loaded" not in ref_loaded:
        raise ValueError("Incomplete load results; refusing state commit")

    any_rows = int(tx_loaded["rows"]) > 0 or int(ref_loaded["rows"]) > 0
    if any_rows and not clickhouse_succeeded:
        raise ValueError("Downstream Silver/Gold/ClickHouse pipeline was not successful; refusing watermark commit")

    windows = plan.get("transaction_windows")
    if not isinstance(windows, dict) or not windows:
        raise ValueError("Missing transaction windows; refusing watermark commit")
    updates = {}
    for table, window in windows.items():
        if not isinstance(window, dict) or not window.get("to"):
            raise ValueError(f"Invalid watermark window for {table}; refusing state commit")
        updates[f"wm__{table}"] = str(window["to"])

    if ref_loaded["loaded"]:
        if not reference_extract or not reference_extract.get("geolocation_sha256"):
            raise ValueError("Missing reference extraction fingerprint; snapshot commit is unsafe")
        if not ref_loaded.get("snapshot_id"):
            raise ValueError("Missing reference snapshot_id; snapshot commit is unsafe")
        updates["last_reference_snapshot_date"] = str(ref_loaded["snapshot_id"])
        updates["geolocation_snapshot_sha256"] = str(reference_extract["geolocation_sha256"])

    updates["last_pipeline_success_at"] = success_at
    updates["last_transaction_batch_id"] = str(tx_loaded.get("batch_id", ""))
    return updates
