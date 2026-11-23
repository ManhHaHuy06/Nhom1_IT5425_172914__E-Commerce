"""Check the Spark `predict` stage against the offline scoring, order by order.

    python validate_predictions.py --artifacts artifacts --data-dir ../hosting_server/Data

Reads olist_gold.order_predictions from ClickHouse (HTTP, standard library) and scores the same orders offline with the same weights.
late_risk / expected_days / safe_promise_days must agree (this is the only way to verify the Spark SQL that builds the features).
Flags are reported but not enforced: they depend on which days were loaded. Exit code 0 = OK.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from olist_ml.data import load_orders                                        # noqa: E402
from olist_ml.predict_core import RAW_COLUMNS, load_artifacts, predict_orders  # noqa: E402


def clickhouse_rows(url: str, user: str, password: str, sql: str) -> list:
    req = urllib.request.Request(f"{url.rstrip('/')}/?{urllib.parse.urlencode({'database': 'olist_gold'})}", data=(sql + " FORMAT JSONEachRow").encode(),
                                 method="POST", headers={"X-ClickHouse-User": user, "X-ClickHouse-Key": password})
    with urllib.request.urlopen(req, timeout=120) as r:
        return [json.loads(l) for l in r.read().decode().splitlines() if l.strip()]


def compare(rows: list, offline: pd.DataFrame, tol: float = 1e-6) -> dict:
    ch = pd.DataFrame(rows).set_index("order_id") if rows else pd.DataFrame()
    off = offline.set_index("order_id")
    both = ch.index.intersection(off.index) if len(ch) else ch.index
    res = {"rows_clickhouse": len(ch), "rows_offline": len(off), "compared": len(both), "only_in_clickhouse": int(len(ch.index.difference(off.index))) if len(ch) else 0, "bad": {}}
    for c in ("late_risk", "expected_days", "safe_promise_days"):
        if len(both):
            d = np.abs(pd.to_numeric(ch.loc[both, c]).values - off.loc[both, c].values)
            res["bad"][c] = {"mismatches": int((d > tol).sum()), "max_abs_diff": float(d.max())} if (d > tol).any() else None
    res["flag_disagreement"] = float((pd.to_numeric(ch.loc[both, "is_flagged"]).values != off.loc[both, "is_flagged"].values).mean()) if len(both) else None
    res["ok"] = len(both) > 0 and res["only_in_clickhouse"] == 0 and not any(res["bad"].values())
    return res


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifacts", default="artifacts"); ap.add_argument("--data-dir", required=True)
    ap.add_argument("--url", default=os.getenv("CLICKHOUSE_URL", "http://localhost:8123")); ap.add_argument("--user", default=os.getenv("CLICKHOUSE_USER", "default"))
    ap.add_argument("--password", default=os.getenv("CLICKHOUSE_PASSWORD", ""))
    a = ap.parse_args(argv)
    art_dir = Path(a.artifacts)
    rows = clickhouse_rows(a.url, a.user, a.password, "SELECT order_id, late_risk, expected_days, safe_promise_days, is_flagged, model_version FROM order_predictions")
    version = rows[0]["model_version"] if rows else "?"
    art = load_artifacts((art_dir / "late_risk.joblib").read_bytes(), (art_dir / "forecast_bands.json").read_text(),
                         (art_dir / "thresholds.json").read_text(), (art_dir / "model_card.json").read_text(), version)
    raw = load_orders(a.data_dir)[[c for c in RAW_COLUMNS if c != "order_key"]]
    r = compare(rows, predict_orders(raw, art, datetime.utcnow()))
    for k, v in r.items():
        print(f"{k}: {v}")
    print("OK: Spark predictions match the offline scoring" if r["ok"] else "DIFFERENCES FOUND")
    return 0 if r["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
