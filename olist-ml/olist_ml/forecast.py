"""28-day-ahead forecast of daily orders (global model over (forecast-origin, horizon) pairs) with empirical bands.

Every input uses information up to the forecast origin only; the target is y[t+h] / mean28[t] (scale-free), and the
prediction is the average of a "level x weekday profile" baseline and a gradient-boosting model.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor

HORIZON, WINSOR, SEED = 28, (0.2, 2.5), 42
NATIONAL_HOLIDAYS = pd.to_datetime([
    "2016-09-07", "2016-10-12", "2016-11-02", "2016-11-15", "2016-12-25",
    "2017-01-01", "2017-02-28", "2017-04-14", "2017-04-21", "2017-05-01", "2017-06-15", "2017-09-07", "2017-10-12", "2017-11-02", "2017-11-15", "2017-12-25",
    "2018-01-01", "2018-02-13", "2018-03-30", "2018-04-21", "2018-05-01", "2018-05-31", "2018-09-07", "2018-10-12", "2018-11-02", "2018-11-15", "2018-12-25"])
BLACK_FRIDAY = pd.to_datetime(["2017-11-24", "2018-11-23"])
RETAIL_EVENTS = pd.to_datetime(["2017-05-14", "2018-05-13", "2017-06-12", "2018-06-12", "2017-08-13", "2018-08-12",
                                "2017-10-12", "2018-10-12", "2017-12-24", "2017-12-25"])
WC2018_BRAZIL = pd.to_datetime(["2018-06-17", "2018-06-22", "2018-06-27", "2018-07-02", "2018-07-06"])
FEATURES = ["h", "r1", "r7", "r14", "r56", "cv7", "wd4_ratio", "prof", "dow", "dom", "month", "is_hol", "is_event", "bf_week", "wc_brazil"]


def calendar_features(dates) -> pd.DataFrame:
    d = pd.DatetimeIndex(dates)
    out = pd.DataFrame({"dow": d.dayofweek, "dom": d.day, "month": d.month}, index=d)
    out["is_hol"], out["is_event"] = d.isin(NATIONAL_HOLIDAYS).astype(int), d.isin(RETAIL_EVENTS).astype(int)
    out["bf_week"] = 0
    for b in BLACK_FRIDAY:
        out["bf_week"] |= (np.abs((d - b).days) <= 3).astype(int)
    out["wc_brazil"] = d.isin(WC2018_BRAZIL).astype(int)
    return out.reset_index(drop=True)


def build_samples(y: pd.Series, origins, horizon: int = HORIZON) -> pd.DataFrame:
    """One row per (origin t, horizon h). Inputs use days <= t only. Targets beyond the end of y are NaN."""
    v = y.values.astype(float); n = len(v); dates = y.index; s = pd.Series(v)
    m7, m14, m28, m56 = (s.rolling(w).mean().values for w in (7, 14, 28, 56))
    sd7 = s.rolling(7).std().values
    dow_all = dates.dayofweek.values
    prof = np.full((n, 7), np.nan)
    for t in range(55, n):
        w, dw = v[t - 55:t + 1], dow_all[t - 55:t + 1]
        for d in range(7):
            prof[t, d] = w[dw == d].mean() / w.mean()
    O = np.asarray(list(origins)); Hs = np.arange(1, horizon + 1)
    T, Hh = np.repeat(O, len(Hs)), np.tile(Hs, len(O)); tgt = T + Hh
    tgt_dates = dates[0] + pd.to_timedelta(tgt, unit="D")
    k0 = np.ceil(Hh / 7).astype(int)
    lagged = np.stack([np.where(tgt - 7 * (k0 + j) >= 0, v[np.clip(tgt - 7 * (k0 + j), 0, n - 1)], np.nan) for j in range(4)], axis=1)
    cal = calendar_features(tgt_dates)
    S = pd.DataFrame({"origin_idx": T, "tgt_idx": tgt, "h": Hh, "date": tgt_dates,
                      "y": np.where(tgt < n, v[np.clip(tgt, 0, n - 1)], np.nan), "m28": m28[T],
                      "r1": v[T] / m28[T], "r7": m7[T] / m28[T], "r14": m14[T] / m28[T], "r56": m56[T] / m28[T],
                      "cv7": sd7[T] / m7[T], "wd4_ratio": np.nanmean(lagged, axis=1) / m28[T], "prof": prof[T, cal["dow"].values]})
    S = pd.concat([S, cal], axis=1)
    S["target_ratio"] = S.y / S.m28
    S["feat_ok"] = S[["m28", "r56", "prof", "wd4_ratio"]].notna().all(axis=1) & (S.m28 > 0)
    return S


class DailyForecaster:
    def __init__(self, horizon: int = HORIZON):
        self.horizon = horizon

    def forecast(self, y: pd.Series, origin=None) -> pd.Series:
        """Forecast the `horizon` days after `origin` (default: last day of y), using only data up to `origin`."""
        if origin is not None:
            y = y.loc[:pd.Timestamp(origin)]
        n = len(y)
        if n < 60:
            raise ValueError("need at least 60 days of history")
        S = build_samples(y, range(55, n), self.horizon)
        tr = S[(S.tgt_idx <= n - 1) & S.feat_ok]
        te = S[S.origin_idx == n - 1].sort_values("h")
        mdl = HistGradientBoostingRegressor(loss="absolute_error", learning_rate=0.05, max_iter=250, max_depth=4,
                                            min_samples_leaf=40, l2_regularization=1.0, random_state=SEED)
        mdl.fit(tr[FEATURES], tr.target_ratio.clip(*WINSOR))
        level = te.m28.values
        pred = 0.5 * level * te.prof.values + 0.5 * level * mdl.predict(te[FEATURES])
        idx = pd.date_range(y.index[-1] + pd.Timedelta(days=1), periods=self.horizon, freq="D")
        return pd.Series(np.clip(pred, 0, None), index=idx, name="forecast")

    def rolling(self, y: pd.Series, origins) -> pd.DataFrame:
        """Re-forecast from several origins (what the dashboard does every simulated day). Columns: origin, date, h, forecast."""
        rows = []
        for o in origins:
            f = self.forecast(y, o)
            rows.append(pd.DataFrame({"origin": pd.Timestamp(o), "date": f.index, "h": np.arange(1, len(f) + 1), "forecast": f.values}))
        return pd.concat(rows, ignore_index=True)

    def fit_bands(self, y: pd.Series, n_folds: int = 5) -> dict:
        """Backtest on the last `n_folds` windows of y; returns the 10%/90% quantiles of actual/forecast per horizon week."""
        n, H = len(y), self.horizon
        ratios, folds = [], []
        for k in range(n_folds, 0, -1):
            T0 = n - 1 - k * H
            f = self.forecast(y.iloc[:T0 + 1]); a = y.iloc[T0 + 1:T0 + 1 + H]
            ratios.append(pd.DataFrame({"h": np.arange(1, H + 1), "r": (a.values / f.values)}))
            folds.append({"origin": str(y.index[T0].date()), "wape": float(np.abs(f.values - a.values).sum() / a.values.sum()),
                          "total_error": float(f.values.sum() / a.values.sum() - 1)})
        R = pd.concat(ratios).replace([np.inf, -np.inf], np.nan).dropna()
        wk = pd.cut(R.h, [0, 7, 14, 21, 28], labels=False)
        q = R.groupby(wk).r.quantile([0.1, 0.9]).unstack()
        return {"q": [[float(q.loc[i, 0.1]), float(q.loc[i, 0.9])] for i in range(4)], "folds": folds}
