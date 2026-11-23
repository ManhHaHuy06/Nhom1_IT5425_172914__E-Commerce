"""Run from olist-ml:  python -m pytest -q   (trains a model on the fly, ~30 s)"""
import os
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from olist_ml import train                                        # noqa: E402
from olist_ml.data import daily_series, load_orders               # noqa: E402
from olist_ml.forecast import DailyForecaster                     # noqa: E402
from olist_ml.predict_core import (FORECAST_COLUMNS, ORDER_PRED_COLUMNS, RAW_COLUMNS, daily_orders_series, forecast_orders,  # noqa: E402
                                   load_artifacts, predict_orders, to_rows)
from olist_ml.registry import runtime_versions, version_mismatches   # noqa: E402

DATA = Path(os.getenv("OLIST_DATA_DIR", ROOT.parent / "hosting_server" / "Data"))
NOW = datetime(2018, 7, 1, 12, 0, 0)


@pytest.fixture(scope="module")
def art():
    out = Path(tempfile.mkdtemp())
    train.main(["--data-dir", str(DATA), "--out", str(out), "--train-end", "2018-06-01"])
    return load_artifacts((out / "late_risk.joblib").read_bytes(), (out / "forecast_bands.json").read_text(),
                          (out / "thresholds.json").read_text(), (out / "model_card.json").read_text(), "v1_test")


@pytest.fixture(scope="module")
def raw():
    o = load_orders(DATA)
    r = o[[c for c in RAW_COLUMNS if c != "order_key"]].copy()
    r.insert(1, "order_key", np.arange(len(r)) + 1)                # what the Spark query returns
    return r


def window(raw, a, b):
    t = raw.order_purchase_timestamp
    return raw[(t >= a) & (t < b)]


def test_predictions_follow_the_contract(raw, art):
    w = window(raw, "2018-06-01", "2018-06-29")
    p = predict_orders(w, art, NOW)
    assert list(p.columns) == ORDER_PRED_COLUMNS and len(p) == len(w) and p.order_id.is_unique
    assert p.late_risk.between(0, 1).all() and p.drop(columns=["order_key"]).notna().all().all()
    assert (p.model_version == "v1_test").all() and (p.scored_at == pd.Timestamp(NOW)).all()
    assert p.purchase_date.is_monotonic_increasing and (p.is_escalated <= p.is_flagged).all()
    sales = w[~w.order_status.isin(["canceled", "unavailable"])]
    later = p[p.order_id.isin(sales[sales.order_purchase_timestamp >= "2018-06-04"].order_id)]
    assert 0.06 < later.is_flagged.mean() < 0.16 and 0.01 < later.is_escalated.mean() < 0.05     # top ~10 % / ~2.5 %


def test_canceled_orders_are_scored_but_never_flagged(raw, art):
    w = window(raw, "2018-06-01", "2018-06-29"); p = predict_orders(w, art, NOW)
    off = p[p.order_id.isin(w[w.order_status.isin(["canceled", "unavailable"])].order_id)]
    assert len(off) > 0 and (off.is_flagged == 0).all() and off.late_risk.notna().all()


def test_no_look_ahead_adding_later_orders_changes_nothing_for_earlier_ones(raw, art):
    short = predict_orders(window(raw, "2018-06-01", "2018-06-11"), art, NOW).set_index("order_id")
    long = predict_orders(window(raw, "2018-06-01", "2018-06-21"), art, NOW).set_index("order_id").loc[short.index]
    pd.testing.assert_frame_equal(short.drop(columns=["scored_at"]), long.drop(columns=["scored_at"]))


def test_idempotent_and_robust_to_missing_geolocation(raw, art):
    w = window(raw, "2018-06-01", "2018-06-08").copy()
    a = predict_orders(w, art, NOW); b = predict_orders(w, art, NOW)
    pd.testing.assert_frame_equal(a, b)
    w.loc[w.index[:40], ["c_lat", "c_lng", "s_lat"]] = np.nan
    assert predict_orders(w, art, NOW).late_risk.notna().all()


def test_missing_input_columns_fail_loudly(raw, art):
    with pytest.raises(ValueError):
        predict_orders(window(raw, "2018-06-01", "2018-06-03").drop(columns=["price"]), art, NOW)


def test_forecast_follows_the_contract_and_uses_the_last_complete_day(raw, art):
    daily = daily_orders_series(raw)
    f = forecast_orders(daily, art, NOW)
    last = daily.index.max().date()
    assert list(f.columns) == FORECAST_COLUMNS and len(f) == 28 and (f.level == "ALL").all()
    assert f.origin_date.iloc[0] == date.fromordinal(last.toordinal() - 1)               # newest day is dropped: it may be incomplete
    assert list(f.horizon_day) == list(range(1, 29)) and f.target_date.iloc[0] == date.fromordinal(f.origin_date.iloc[0].toordinal() + 1)
    assert (f.lo80 <= f.forecast).all() and (f.forecast <= f.hi80).all() and (f.forecast >= 0).all()
    ref = DailyForecaster().forecast(daily.loc["2017-01-05":pd.Timestamp(f.origin_date.iloc[0])])
    np.testing.assert_allclose(f.forecast.values, ref.values)
    g = forecast_orders(daily, art, NOW, complete_through="2018-06-25")
    assert g.origin_date.iloc[0] == date(2018, 6, 25)
    assert forecast_orders(daily.loc["2018-01-01":"2018-02-15"], art, NOW).empty                # < 60 days: empty, not a crash


def test_daily_series_matches_the_training_loader(raw):
    a = daily_orders_series(raw).loc["2017-01-05":"2018-08-20"]
    b = daily_series(load_orders(DATA)).loc["2017-01-05":"2018-08-20"]
    pd.testing.assert_series_equal(a, b, check_names=False, check_freq=False)


def test_rows_are_plain_python_types_for_spark(raw, art):
    p = predict_orders(window(raw, "2018-06-01", "2018-06-05"), art, NOW)
    p.loc[p.index[0], "late_risk"] = np.nan
    rows = to_rows(p)
    ok = (int, float, str, date, datetime, type(None))
    assert all(isinstance(v, ok) and not isinstance(v, np.generic) for r in rows for v in r)
    assert rows[0][ORDER_PRED_COLUMNS.index("late_risk")] is None and isinstance(rows[1][2], date) and isinstance(rows[1][-1], datetime)
    assert to_rows(pd.DataFrame(columns=ORDER_PRED_COLUMNS)) == []


def test_library_version_guard():
    rt = runtime_versions()
    assert version_mismatches({"versions": rt}) == []
    other = {"versions": {**rt, "scikit-learn": "0.20.1"}}
    assert "scikit-learn" in version_mismatches(other)[0]
    assert version_mismatches({"versions": {**rt, "scikit-learn": rt["scikit-learn"].rsplit(".", 1)[0] + ".99"}}) == []   # patch level may differ


def test_validation_tool_accepts_a_faithful_spark_output_and_catches_a_wrong_one(raw, art):
    sys.path.insert(0, str(ROOT))
    import validate_predictions as vp
    off = predict_orders(window(raw, "2018-06-01", "2018-06-08"), art, NOW)
    rows = [{k: (v.item() if hasattr(v, "item") else v) for k, v in r.items()} for r in off[["order_id", "late_risk", "expected_days", "safe_promise_days", "is_flagged", "model_version"]].to_dict("records")]
    good = vp.compare(rows, off)
    assert good["ok"] and good["compared"] == len(off) and good["flag_disagreement"] == 0
    bad = [dict(r) for r in rows]; bad[3]["late_risk"] += 0.01; bad.append({**rows[0], "order_id": "ghost"})      # a wrong feature + an order that does not exist
    r = vp.compare(bad, off)
    assert not r["ok"] and r["bad"]["late_risk"]["mismatches"] == 1 and r["only_in_clickhouse"] == 1
    assert not vp.compare([], off)["ok"]
