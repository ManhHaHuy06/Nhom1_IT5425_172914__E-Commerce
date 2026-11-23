"""Rank-based action rule (no look-ahead): flag = top 10 %, escalate = top 2.5 % of the risk scores of the orders that arrived in
the previous 3 days (static fallback until `min_orders` exist). Rank-based on purpose: the late rate drifts (7 % -> 1.4 %), so a
fixed percentage cut-off would drift with it."""
from __future__ import annotations

import numpy as np

TRAIL = 3 * 1440


def action_flags(minutes, risk, start, flag_pct, esc_pct, min_orders=100):
    """Rows start..n-1. `minutes` must be sorted; row i only looks at rows before i. risk and thresholds are in percent."""
    n = len(minutes)
    lo = np.searchsorted(minutes, minutes - TRAIL, side="left")
    thr, esc = np.full(n - start, flag_pct, float), np.full(n - start, esc_pct, float)
    for j, i in enumerate(range(start, n)):
        if i - lo[i] >= min_orders:
            thr[j], esc[j] = np.quantile(risk[lo[i]:i], [0.90, 0.975])
    r = risk[start:]
    return thr, (r >= thr).astype(int), (r >= esc).astype(int)
