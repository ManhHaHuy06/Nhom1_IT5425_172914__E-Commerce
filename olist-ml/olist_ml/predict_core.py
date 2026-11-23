"""Pure-pandas core of the Spark stage `predict` (the Spark wrapper only moves data in and out, so this part is unit-tested)."""
from __future__ import annotations

import json
from dataclasses import dataclass

import numpy as np
import pandas as pd

from .data import NON_SALE, add_features
from .flags import action_flags
from .forecast import HORIZON, DailyForecaster
from .late_risk import LateRiskModel

# Column ORDER is a contract with layers/gold/create_gold_tables.sql and layers/clickhouse/init_gold.sql (tests check it).
ORDER_PRED_COLUMNS = ["order_id", "order_key", "purchase_date", "late_risk", "expected_days", "safe_promise_days",
                      "is_flagged", "is_escalated", "model_version", "scored_at"]
FORECAST_COLUMNS = ["level", "origin_date", "target_date", "horizon_day", "forecast", "lo80", "hi80", "model_version", "scored_at"]
# Columns the Spark query (layers/predict/build_predictions.py: order_features) must return; a contract test compares them.
RAW_COLUMNS = ["order_id", "order_key", "order_status", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
               "order_delivered_customer_date", "order_estimated_delivery_date", "customer_state", "c_lat", "c_lng", "seller_id",
               "seller_state", "s_lat", "s_lng", "category", "payment_type", "installments", "price", "freight", "n_items",
               "n_sellers", "weight_g", "vol_cm3"]
MIN_HISTORY_DAYS = 60
FORECAST_START = "2017-01-05"        # the first months of the dataset are sparse (start-up period)


@dataclass
class Artifacts:
    model: LateRiskModel
    bands: dict
    thresholds: dict
    card: dict
    version: str


def load_artifacts(model_bytes: bytes, bands_json: str, thresholds_json: str, card_json: str, version: str) -> Artifacts:
    return Artifacts(LateRiskModel.from_bytes(model_bytes), json.loads(bands_json), json.loads(thresholds_json), json.loads(card_json), version)


def predict_orders(raw: pd.DataFrame, art: Artifacts, scored_at) -> pd.DataFrame:
    """One row per order. `raw` = order-level frame with the raw columns of olist_ml.data (+ order_key).
    Scores use purchase-time information only. Flags are computed over SALES (not canceled/unavailable) in time order with the
    trailing 3-day rank rule; other orders are scored but never flagged."""
    missing = [c for c in RAW_COLUMNS if c != "order_key" and c not in raw.columns]
    if missing:
        raise ValueError(f"missing input columns: {missing}")
    raw = raw.reset_index(drop=True)
    df = add_features(raw)
    sc = art.model.score(df)
    sales = ~df.order_status.isin(NON_SALE)
    flag, esc = np.zeros(len(df), int), np.zeros(len(df), int)
    idx = df[sales].sort_values(["order_purchase_timestamp", "order_id"]).index
    if len(idx):
        ts = df.loc[idx, "order_purchase_timestamp"]
        minutes = ((ts - ts.iloc[0]).dt.total_seconds() // 60).astype(int).values
        _, f, e = action_flags(minutes, sc.late_risk.loc[idx].values * 100, 0, art.thresholds["flag_pct"], art.thresholds["escalate_pct"])
        flag[idx], esc[idx] = f, e
    out = pd.DataFrame({
        "order_id": df.order_id.values, "order_key": raw["order_key"].values if "order_key" in raw else pd.NA,
        "purchase_date": df.date.dt.date.values, "late_risk": sc.late_risk.values, "expected_days": sc.expected_days.values,
        "safe_promise_days": sc.safe_promise_days.values, "is_flagged": flag, "is_escalated": esc,
        "model_version": art.version, "scored_at": pd.Timestamp(scored_at)})
    return out.sort_values(["purchase_date", "order_id"]).reset_index(drop=True)[ORDER_PRED_COLUMNS]


def daily_orders_series(raw: pd.DataFrame) -> pd.Series:
    """Orders that became sales, per purchase day (same definition as olist_ml.data.daily_series)."""
    v = raw[~raw.order_status.isin(NON_SALE)]
    d = pd.to_datetime(v.order_purchase_timestamp).dt.normalize()
    s = d.value_counts().sort_index()
    return s.reindex(pd.date_range(s.index.min(), s.index.max(), freq="D"), fill_value=0).astype(float).rename("orders")


def forecast_orders(daily: pd.Series, art: Artifacts, scored_at, complete_through=None, level: str = "ALL") -> pd.DataFrame:
    """28-day forecast of daily orders from the last COMPLETE day. The newest day in the data is usually still filling up, so by
    default it is dropped (complete_through = last day - 1). Returns an empty frame if there is not enough history.
    The 80 % band comes from the backtest ratios; it is widened (never narrowed) so that lo80 <= forecast <= hi80."""
    empty = pd.DataFrame(columns=FORECAST_COLUMNS)
    if daily.empty:
        return empty
    y = daily.loc[FORECAST_START:] if daily.index.min() <= pd.Timestamp(FORECAST_START) else daily
    end = pd.Timestamp(complete_through) if complete_through is not None else y.index.max() - pd.Timedelta(days=1)
    y = y.loc[:end]
    if len(y) < MIN_HISTORY_DAYS:
        return empty
    f = DailyForecaster().forecast(y)
    q = np.array([art.bands["q"][j // 7] for j in range(HORIZON)])
    return pd.DataFrame({
        "level": level, "origin_date": y.index[-1].date(), "target_date": [d.date() for d in f.index], "horizon_day": np.arange(1, HORIZON + 1),
        "forecast": f.values, "lo80": np.minimum(f.values * q[:, 0], f.values), "hi80": np.maximum(f.values * q[:, 1], f.values),
        "model_version": art.version, "scored_at": pd.Timestamp(scored_at)})[FORECAST_COLUMNS]


def _py(v):
    if v is None or v is pd.NaT or v is pd.NA:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v.item() if isinstance(v, np.generic) else v


def to_rows(df: pd.DataFrame) -> list:
    """Plain Python tuples (int/float/str/date/datetime/None) for spark.createDataFrame(rows, schema)."""
    return [tuple(_py(v) for v in rec) for rec in df.itertuples(index=False, name=None)]
