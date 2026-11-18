"""Conservative cleanup of only the current Airflow run's local extraction directory."""
from __future__ import annotations

import shutil
from pathlib import Path


def cleanup_run_extracts(work_root: str, run_id: str, transaction_extract: dict | None,
                          reference_extract: dict | None) -> str:
    from ingestion import safe_run_id

    root = Path(work_root).resolve()
    run_candidate = root / safe_run_id(run_id)
    if run_candidate.is_symlink():
        raise ValueError(f"Refusing cleanup of a symlink run directory: {run_candidate}")
    run_dir = run_candidate.resolve()
    if run_dir.parent != root:
        raise ValueError(f"Refusing cleanup outside work root: {run_dir}")

    # XCom paths must resolve to the exact subdirectories created by ingestion.py.
    expected = {
        "transactions": run_dir / "transactions",
        "reference": run_dir / "reference",
    }
    for label, result in (("transactions", transaction_extract), ("reference", reference_extract)):
        supplied = (result or {}).get("work_dir")
        if supplied:
            actual = Path(supplied).resolve()
            if actual != expected[label].resolve():
                raise ValueError(f"Refusing cleanup: {label} XCom path is outside its expected run directory")

    if run_dir.exists():
        if not run_dir.is_dir() or run_dir.is_symlink():
            raise ValueError(f"Refusing cleanup of non-directory or symlink: {run_dir}")
        shutil.rmtree(run_dir)
        return f"cleaned {run_dir.name}"
    return "no local extraction directory to clean"
