"""Sinh biểu thức SQL Trino (phân vị / trung vị chính xác, gán nhãn tầng) và đổi band của Gold sang dạng notebook dùng.

Band của Gold có dạng `[(cận trên, nhãn), …, (None, nhãn phần còn lại)]`:
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


# ---------- Biểu thức SQL Trino ----------
def quantile_sql(col: str, p, where: str | None = None) -> str:
    """Phân vị chính xác (nội suy tuyến tính, như quantile_cont của DuckDB / numpy) cho Trino.

    Trino chỉ có approx_percentile nên tự tính trên mảng đã sắp xếp. p là số → 1 giá trị; p là list → ARRAY.
    where = điều kiện lọc thêm (thay cho `quantile_cont(...) FILTER (WHERE ...)`).
    """
    cond = f"{col} IS NOT NULL" + (f" AND ({where})" if where else "")
    values = f"array_sort(array_agg(CAST({col} AS DOUBLE)) FILTER (WHERE {cond}))"

    def at(prob: str) -> str:
        pos = f"(1 + {prob} * (cardinality(qs) - 1))"
        lo = f"element_at(qs, CAST(floor({pos}) AS INTEGER))"
        hi = f"element_at(qs, CAST(ceil({pos}) AS INTEGER))"
        return f"({lo} + ({pos} - floor({pos})) * ({hi} - {lo}))"

    if isinstance(p, (list, tuple)):
        body = f"transform(ARRAY[{', '.join(f'CAST({v!r} AS DOUBLE)' for v in p)}], qp -> {at('qp')})"
    else:
        body = at(f"CAST({p!r} AS DOUBLE)")
    return f"transform(ARRAY[{values}], qs -> IF(cardinality(qs) = 0, NULL, {body}))[1]"


def median_sql(col: str, where: str | None = None) -> str:
    """Trung vị chính xác (thay cho median() của DuckDB)."""
    return quantile_sql(col, 0.5, where)


def band_sql(col: str, bins: list, last_label: str, upper_inclusive: bool = False, null_label: str | None = None) -> str:
    """Biểu thức CASE gán nhãn tầng. bins = [(cận trên, nhãn), ...] tăng dần; vượt cận cuối → last_label.

    upper_inclusive=False: `col < cận`; True: `col <= cận`. Giá trị NULL → null_label (mặc định NULL).
    """
    op = "<=" if upper_inclusive else "<"
    null_value = f"'{null_label}'" if null_label else "NULL"
    whens = " ".join(f"WHEN {col} {op} {upper} THEN '{label}'" for upper, label in bins)
    return f"CASE WHEN {col} IS NULL THEN {null_value} {whens} ELSE '{last_label}' END"
