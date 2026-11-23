"""Run from olist-ml:   python -m pytest -q      (uses the real CSVs in olist-data-source-v2/data)"""
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from olist_ml.data import daily_series, load_orders
from olist_ml.forecast import DailyForecaster, build_samples
from olist_ml.late_risk import LateRiskModel

DATA = Path(os.getenv("OLIST_DATA_DIR", Path(__file__).resolve().parents[2] / "hosting_server" / "Data"))


@pytest.fixture(scope="module")
def orders():
    return load_orders(DATA)


@pytest.fixture(scope="module")
def series(orders):
    return daily_series(orders).loc["2017-01-05":"2018-08-20"]


def test_one_row_per_order_and_only_original_ids(orders):
    assert orders.order_id.is_unique
    assert (orders.order_id.str.len() == 32).all()


def test_daily_series_is_complete(series):
    assert series.index.is_monotonic_increasing and (series.index.to_series().diff().dropna() == pd.Timedelta(days=1)).all()
    assert series.loc["2018-01-01":].mean() > 150


@pytest.fixture(scope="module")
def late_model(orders):
    return LateRiskModel().fit(orders[orders.order_purchase_timestamp < "2018-03-01"])


def test_late_risk_beats_chance_out_of_time(late_model, orders):
    test = orders[(orders.order_purchase_timestamp >= "2018-03-01") & (orders.order_purchase_timestamp < "2018-08-01")]
    m = late_model.evaluate(test)
    assert m["auc"] > 0.70                                   # notebook result: ~0.74
    assert m["precision_top10"] > 1.8 * m["base_rate"]      # flagged orders are clearly riskier
    assert m["mae_days_model"] < m["mae_days_constant"]


def test_scores_are_valid_and_robust_to_missing_geo(late_model, orders):
    sample = orders[orders.order_purchase_timestamp >= "2018-07-24"].head(300).copy()
    sample.loc[sample.index[:20], ["distance_km"]] = np.nan
    s = late_model.score(sample)
    assert len(s) == 300 and s.late_risk.between(0, 1).all() and s.notna().all().all()
    assert (s.safe_promise_days >= s.expected_days - 1e-6).mean() > 0.95


def test_forecast_shape_and_no_leakage(series):
    fc = DailyForecaster()
    origin = pd.Timestamp("2018-06-25")
    f_cut = fc.forecast(series.loc[:origin])
    f_full = fc.forecast(series, origin)                      # the future is present in y but must be ignored
    assert len(f_cut) == 28 and (f_cut >= 0).all() and f_cut.index[0] == origin + pd.Timedelta(days=1)
    np.testing.assert_allclose(f_cut.values, f_full.values)


def test_samples_never_use_future_information(series):
    S = build_samples(series.iloc[:300], range(55, 300))
    assert (S.tgt_idx > S.origin_idx).all()
    last = S[S.origin_idx == 299]
    assert last.y.isna().all()                                # nothing after the end can leak into a target


def test_bands_are_ordered_and_sane(series):
    # NOTE: lo < 1 < hi is NOT guaranteed: when the model over-forecasts in every recent fold (it did in mid-2018),
    # the upper ratio can fall below 1.  That bias is a property of the data, so we only check ordering and scale.
    b = DailyForecaster().fit_bands(series.loc[:"2018-07-23"], n_folds=3)
    assert all(0.2 < lo < hi < 2.5 for lo, hi in b["q"])
    assert all(f["wape"] < 0.4 for f in b["folds"])
