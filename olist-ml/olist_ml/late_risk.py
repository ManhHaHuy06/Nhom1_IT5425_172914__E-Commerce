"""Order-level models scored at PURCHASE time: late-delivery risk, expected delivery days, safe promise.

Only information known when the order is placed is used (price, freight, distance, promised days, calendar, seller
history).  Seller history is a smoothed target encoding: out-of-fold on the training set, full table at scoring time.
"""
from __future__ import annotations

import io

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.metrics import average_precision_score, mean_absolute_error, roc_auc_score
from sklearn.model_selection import KFold

NUM = ["price", "freight", "freight_ratio", "n_items", "n_sellers", "weight_g", "vol_cm3", "distance_km", "same_state",
       "hour", "dow", "month", "est_days", "installments"]
CATS = ["customer_state", "seller_state", "category", "payment_type"]
PRIOR_W, SEED = 20, 42


def eligible(df: pd.DataFrame) -> pd.DataFrame:
    """Delivered orders whose outcome is known (and with a usable distance, for training)."""
    return df[(df.order_status == "delivered") & df.order_delivered_customer_date.notna()].copy()


class LateRiskModel:
    PROMISE_Q = 0.92

    def _codes(self, df: pd.DataFrame) -> pd.DataFrame:
        out = pd.DataFrame(index=df.index)
        for c in CATS:
            s = df[c].fillna("NA")
            if c == "category":
                s = s.where(s.isin(self.vocab[c]), "other")
            out[c] = s.map({v: i for i, v in enumerate(self.vocab[c])}).fillna(len(self.vocab[c])).astype(int)
        return out

    def _seller_te(self, df: pd.DataFrame):
        g = self.seller_tab
        rate = (df.seller_id.map(g["sum"]).fillna(0) + self.prior * PRIOR_W) / (df.seller_id.map(g["count"]).fillna(0) + PRIOR_W)
        return rate.values, df.seller_id.map(g["count"]).fillna(0).values

    def _X(self, df, rate=None, n=None) -> pd.DataFrame:
        if rate is None:
            rate, n = self._seller_te(df)
        X = pd.concat([df[NUM], self._codes(df)], axis=1)
        X["seller_late"], X["seller_n"] = rate, n
        return X

    def fit(self, train: pd.DataFrame) -> "LateRiskModel":
        tr = eligible(train).dropna(subset=["distance_km"])
        top = list(tr.category.value_counts().index[:30])
        self.vocab = {"customer_state": sorted(tr.customer_state.dropna().unique()), "seller_state": sorted(tr.seller_state.dropna().unique()),
                      "payment_type": sorted(tr.payment_type.dropna().unique()) + ["NA"], "category": sorted(top) + ["other"]}
        self.prior = float(tr.late.mean())
        # out-of-fold seller encoding for training rows (a row never sees its own label)
        rate, n = np.zeros(len(tr)), np.zeros(len(tr))
        for a, b in KFold(5, shuffle=True, random_state=SEED).split(tr):
            g = tr.iloc[a].groupby("seller_id").late.agg(["sum", "count"])
            dst = tr.iloc[b]
            rate[b] = ((dst.seller_id.map(g["sum"]).fillna(0) + self.prior * PRIOR_W) / (dst.seller_id.map(g["count"]).fillna(0) + PRIOR_W)).values
            n[b] = dst.seller_id.map(g["count"]).fillna(0).values
        self.seller_tab = tr.groupby("seller_id").late.agg(["sum", "count"])
        X = self._X(tr, rate, n)
        cat_idx = [list(X.columns).index(c) for c in CATS]
        mk = lambda: dict(learning_rate=0.05, max_iter=250, max_depth=5, min_samples_leaf=60, l2_regularization=1.0,
                          categorical_features=cat_idx, random_state=SEED)
        self.clf = HistGradientBoostingClassifier(**mk()).fit(X, tr.late)
        self.days_cols = [c for c in NUM if c != "est_days"] + CATS          # the platform's own promise is NOT an input here
        cidx = [self.days_cols.index(c) for c in CATS]
        mr = lambda **k: HistGradientBoostingRegressor(learning_rate=0.05, max_iter=250, max_depth=5, min_samples_leaf=60,
                                                       l2_regularization=1.0, categorical_features=cidx, random_state=SEED, **k)
        Xd = X[self.days_cols]
        self.median = mr(loss="absolute_error").fit(Xd, tr.delivery_days)
        self.safe = mr(loss="quantile", quantile=self.PROMISE_Q).fit(Xd, tr.delivery_days)
        return self

    def score(self, df: pd.DataFrame) -> pd.DataFrame:
        """One row per order: late_risk (0-1), expected_days (median), safe_promise_days (92nd percentile)."""
        X = self._X(df)
        Xd = X[self.days_cols]
        return pd.DataFrame({"order_id": df.order_id.values,
                             "late_risk": self.clf.predict_proba(X)[:, 1],
                             "expected_days": self.median.predict(Xd),
                             "safe_promise_days": self.safe.predict(Xd)}, index=df.index)

    def evaluate(self, test: pd.DataFrame) -> dict:
        te = eligible(test).dropna(subset=["distance_km"])
        s = self.score(te)
        thr = s.late_risk.quantile(0.90); flag = s.late_risk >= thr
        return {"n": int(len(te)), "base_rate": float(te.late.mean()), "auc": float(roc_auc_score(te.late, s.late_risk)),
                "ap": float(average_precision_score(te.late, s.late_risk)),
                "precision_top10": float(te.late[flag].mean()), "recall_top10": float(te.late[flag].sum() / te.late.sum()),
                "mae_days_model": float(mean_absolute_error(te.delivery_days, s.expected_days)),
                "mae_days_constant": float(mean_absolute_error(te.delivery_days, np.full(len(te), te.delivery_days.median()))),
                "late_vs_safe_promise": float((te.delivery_days > s.safe_promise_days).mean()),
                "late_vs_platform_promise": float((te.delivery_days > te.est_days).mean())}

    def save(self, path): joblib.dump(self, path)

    @staticmethod
    def from_bytes(b: bytes) -> "LateRiskModel":
        """Load from the raw bytes of late_risk.joblib (how the Spark stage reads it from MinIO)."""
        return joblib.load(io.BytesIO(b))

    @staticmethod
    def load(path) -> "LateRiskModel": return joblib.load(path)
