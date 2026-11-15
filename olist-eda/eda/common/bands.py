"""Đổi band của Gold — `[(cận trên, nhãn), …, (None, nhãn phần còn lại)]` — sang các dạng notebook dùng.

    VALUE_BINS, VALUE_LAST = band_bins(GOLD.ORDER_VALUE_BANDS)   # cho band_sql(col, VALUE_BINS, VALUE_LAST)
    PRICE_EDGES = band_edges(GOLD.PRICE_BANDS)                    # [0, 50, 100, …, inf] cho band_labels / pd.cut
    VALUE_LABELS = band_order(GOLD.ORDER_VALUE_BANDS)             # thứ tự nhãn cho ordered()
"""
import math


def band_bins(bands) -> tuple[list, str]:
    """(danh sách (cận trên, nhãn) đã bỏ phần còn lại, nhãn phần còn lại)."""
    *bins, (last_cut, last_label) = bands
    if last_cut is not None:
        raise ValueError("band cuối của Gold phải có cận trên None (phần còn lại)")
    return list(bins), last_label


def band_edges(bands, low=0) -> list:
    """Các mốc [low, cận 1, …, cận n, inf]."""
    return [low, *(cut for cut, _ in band_bins(bands)[0]), math.inf]


def band_order(bands) -> list:
    """Nhãn theo thứ tự tăng dần (gồm nhãn phần còn lại)."""
    return [label for _, label in bands]
