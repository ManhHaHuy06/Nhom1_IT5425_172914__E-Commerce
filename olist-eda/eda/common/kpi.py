"""Bảng KPI so sánh cùng kỳ (giống nhau ở mọi trang)."""
from typing import NamedTuple

import pandas as pd

from .period import Period


class Kpi(NamedTuple):
    key: str      # tên cột trong dict KPI của notebook
    label: str
    fmt: str      # định dạng trong bảng
    kind: str     # cách so sánh YoY, xem COMPARE


# Mỗi kiểu so sánh: (hàm tính chênh lệch, định dạng)
COMPARE = {
    "pct": (lambda a, b: b / a - 1,     "{:+.1%}"),          # % thay đổi (chỉ số quy mô)
    "pp":  (lambda a, b: (b - a) * 100, "{:+.2f} điểm %"),   # chênh lệch điểm % (chỉ số tỷ lệ)
    "abs": (lambda a, b: b - a,         "{:+.2f}"),          # chênh lệch tuyệt đối
}


def kpi_table(kpis: list, compute, period: Period) -> pd.DataFrame:
    """Bảng KPI: toàn kỳ, cùng kỳ, kỳ này và thay đổi YoY. compute(where_sql) → dict {key: giá trị}."""
    k_all, k_prev, k_cur = compute("TRUE"), compute(period.prev), compute(period.cur)
    rows = []
    for k in kpis:
        diff, diff_fmt = COMPARE[k.kind]
        rows.append({"Chỉ số": k.label, "Toàn kỳ": k.fmt.format(k_all[k.key]),
                     period.label_prev: k.fmt.format(k_prev[k.key]), period.label_cur: k.fmt.format(k_cur[k.key]),
                     "Thay đổi YoY": diff_fmt.format(diff(k_prev[k.key], k_cur[k.key]))})
    return pd.DataFrame(rows).set_index("Chỉ số")
