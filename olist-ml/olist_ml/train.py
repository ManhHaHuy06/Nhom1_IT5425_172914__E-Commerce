"""Train the artifacts.

    python -m olist_ml.train --data-dir <9 Olist CSV files> --out artifacts --train-end 2018-06-01

Everything is fitted on data BEFORE --train-end and evaluated on --train-end .. --end (out-of-time, never a random split).
Run it INSIDE the Spark container (train_model.bat) so that the pickle is written and read with the same scikit-learn.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .data import NON_SALE, daily_series, load_orders
from .forecast import DailyForecaster
from .late_risk import LateRiskModel
from .registry import runtime_versions

START = "2017-01-05"


def main(argv=None) -> dict:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--out", default="artifacts")
    ap.add_argument("--train-end", default="2018-06-01", help="first day NOT used for training (the cut-off T)")
    ap.add_argument("--end", default="2018-08-20", help="last complete day of the data")
    ap.add_argument("--model-version", default=None, help="default: v1_T<train-end>")
    a = ap.parse_args(argv)
    out = Path(a.out); out.mkdir(parents=True, exist_ok=True)
    T, end = pd.Timestamp(a.train_end), pd.Timestamp(a.end)
    version = a.model_version or f"v1_T{a.train_end}"

    df = load_orders(a.data_dir)
    train = df[df.order_purchase_timestamp < T]
    test = df[(df.order_purchase_timestamp >= T) & (df.order_purchase_timestamp < end + pd.Timedelta(days=1))]
    lr = LateRiskModel().fit(train)
    lr.save(out / "late_risk.joblib")
    m_lr = lr.evaluate(test) if len(test) else {}

    # Static fallback thresholds from OUT-OF-SAMPLE scores (a model scored on its own training data is over-confident)
    shadow_cut = T - pd.Timedelta(days=28)
    shadow = LateRiskModel().fit(train[train.order_purchase_timestamp < shadow_cut])
    recent = train[(train.order_purchase_timestamp >= shadow_cut) & ~train.order_status.isin(NON_SALE)]
    rs = shadow.score(recent).late_risk * 100
    thr = {"flag_pct": round(float(rs.quantile(0.90)), 1), "escalate_pct": round(float(rs.quantile(0.975)), 1),
           "definition": "90th / 97.5th percentile of out-of-sample late-risk over the 28 days before T"}
    (out / "thresholds.json").write_text(json.dumps(thr, indent=1))

    bands = DailyForecaster().fit_bands(daily_series(df).loc[START:T - pd.Timedelta(days=1)])
    (out / "forecast_bands.json").write_text(json.dumps(bands, indent=1))

    metrics = {"model_version": version, "train_end": a.train_end, "end": a.end, "n_train_orders": int(len(train)),
               "late_risk": m_lr, "thresholds": thr,
               "forecast": {"folds": bands["folds"], "mean_wape": sum(f["wape"] for f in bands["folds"]) / len(bands["folds"])}}
    (out / "metrics.json").write_text(json.dumps(metrics, indent=1))
    card = {"model_version": version, "train_end": a.train_end, "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "versions": runtime_versions(), "note": "late_risk.joblib is a pickle: read it with the same scikit-learn / numpy minor version"}
    (out / "model_card.json").write_text(json.dumps(card, indent=1))
    print(json.dumps({"version": version, "auc": m_lr.get("auc"), "versions": card["versions"]}, indent=1))
    return metrics


if __name__ == "__main__":
    main()
