"""Kỳ phân tích (cùng quy tắc với Gold `report_period` và DAX `Period Start/End`, `GMV QoQ %`) và bảng KPI so sánh kỳ.

Mặc định kỳ này = 01 → tháng báo cáo (suy từ dữ liệu) của năm mới nhất. Ghi đè bằng biến môi trường
`REPORT_START=YYYY-MM` và/hoặc `REPORT_END=YYYY-MM` — tương đương CEO chọn khoảng ngày trên slicer Power BI
(vd. REPORT_START=2018-01, REPORT_END=2018-03 để xem Q1/2018). Không bao giờ cứng năm/tháng trong notebook.
"""
from __future__ import annotations

import os
from typing import NamedTuple

import pandas as pd


def _month(value) -> pd.Timestamp:
    return pd.Timestamp(value).to_period("M").to_timestamp()


def _d(ts: pd.Timestamp) -> str:
    return ts.strftime("%Y-%m-%d")


def _between(start: pd.Timestamp, end: pd.Timestamp, col: str) -> str:
    return f"{col} BETWEEN DATE '{_d(start)}' AND DATE '{_d(end)}'"


class Period(NamedTuple):
    """Các khoảng thời gian tính theo tháng (ngày 01 của tháng). Điều kiện SQL cần cột `purchase_month` (DATE).

    - kỳ này   : start → end
    - cùng kỳ  : start − 12 tháng → end − 12 tháng (YoY)
    - quý này  : max(đầu quý chứa end, start) → end;  quý trước: cùng khoảng lùi 3 tháng (QoQ)
    - xu hướng : 01/(năm của end − 1) → end
    """
    start: pd.Timestamp
    end: pd.Timestamp

    @classmethod
    def resolve(cls, report_month) -> "Period":
        """Kỳ mặc định từ tháng báo cáo suy từ dữ liệu, rồi áp ghi đè REPORT_START / REPORT_END nếu có."""
        end = _month(os.getenv("REPORT_END") or report_month)
        start = _month(os.getenv("REPORT_START") or f"{end.year}-01-01")
        if start > end or start.year != end.year:
            # biểu đồ so sánh cùng tháng / lũy kế trong notebook giả định kỳ nằm gọn trong một năm
            raise ValueError(f"Kỳ {start:%Y-%m} → {end:%Y-%m} không hợp lệ: cần start ≤ end và cùng một năm")
        return cls(start, end)

    # ---------- mốc ----------
    @property
    def year_cur(self) -> int:
        return self.end.year

    @property
    def year_prev(self) -> int:
        return self.end.year - 1

    @property
    def last_mon(self) -> int:             # tháng báo cáo (1–12)
        return self.end.month

    @property
    def prev_start(self) -> pd.Timestamp:
        return self.start - pd.DateOffset(years=1)

    @property
    def prev_end(self) -> pd.Timestamp:
        return self.end - pd.DateOffset(years=1)

    @property
    def q_start(self) -> pd.Timestamp:
        return max(self.end.to_period("Q").start_time, self.start)

    @property
    def pq_start(self) -> pd.Timestamp:
        return self.q_start - pd.DateOffset(months=3)

    @property
    def pq_end(self) -> pd.Timestamp:
        return self.end - pd.DateOffset(months=3)

    @property
    def m_start(self) -> str:             # tháng đầu cửa sổ xu hướng (chuỗi DATE)
        return f"{self.year_prev}-01-01"

    @property
    def m_end(self) -> str:               # tháng báo cáo (chuỗi DATE, ngày 01)
        return _d(self.end)

    @property
    def cur_start(self) -> str:
        return _d(self.start)

    # ---------- điều kiện SQL (cột purchase_month) ----------
    def cur_on(self, col: str = "purchase_month") -> str:
        return _between(self.start, self.end, col)

    def prev_on(self, col: str = "purchase_month") -> str:
        return _between(self.prev_start, self.prev_end, col)

    @property
    def cur(self) -> str:                 # kỳ này
        return self.cur_on()

    @property
    def prev(self) -> str:                # cùng kỳ năm trước
        return self.prev_on()

    @property
    def qtd(self) -> str:                 # quý này (đến hết kỳ)
        return _between(self.q_start, self.end, "purchase_month")

    @property
    def pqtd(self) -> str:                # cùng khoảng của quý trước
        return _between(self.pq_start, self.pq_end, "purchase_month")

    @property
    def in_period(self) -> str:           # cửa sổ xu hướng
        return f"purchase_month BETWEEN DATE '{self.m_start}' AND DATE '{self.m_end}'"

    # ---------- nhãn ----------
    @staticmethod
    def _label(start: pd.Timestamp, end: pd.Timestamp) -> str:
        if start.year == end.year:
            return f"{start:%m}–{end:%m/%Y}" if start != end else f"{end:%m/%Y}"
        return f"{start:%m/%Y}–{end:%m/%Y}"

    def label(self, year: int) -> str:    # cùng các tháng của kỳ này nhưng ở năm `year`, vd. "01–08/2017"
        shift = pd.DateOffset(years=self.end.year - year)
        return self._label(self.start - shift, self.end - shift)

    @property
    def label_cur(self) -> str:
        return self._label(self.start, self.end)

    @property
    def label_prev(self) -> str:
        return self._label(self.prev_start, self.prev_end)

    @property
    def label_q(self) -> str:
        return self._label(self.q_start, self.end)

    @property
    def label_pq(self) -> str:
        return self._label(self.pq_start, self.pq_end)

    @property
    def label_trend(self) -> str:         # "01/2017–08/2018"
        return f"01/{self.year_prev}–{self.end:%m/%Y}"

    def __str__(self) -> str:
        return (f"Kỳ này {self.label_cur} | cùng kỳ {self.label_prev} | quý {self.label_q} vs {self.label_pq} | "
                f"xu hướng {self.label_trend}")


# ---------- Bảng KPI so sánh kỳ ----------
class Kpi(NamedTuple):
    key: str      # tên cột trong dict KPI của notebook
    label: str
    fmt: str      # định dạng trong bảng
    kind: str     # cách so sánh, xem COMPARE


# Mỗi kiểu so sánh: (hàm tính chênh lệch, định dạng)
COMPARE = {
    "pct": (lambda a, b: b / a - 1 if a else float("nan"), "{:+.1%}"),   # % thay đổi (chỉ số quy mô)
    "pp":  (lambda a, b: (b - a) * 100,                    "{:+.2f} điểm %"),  # chênh lệch điểm % (tỷ lệ)
    "abs": (lambda a, b: b - a,                            "{:+.2f}"),    # chênh lệch tuyệt đối
}


def kpi_table(kpis: list, compute, period: Period) -> pd.DataFrame:
    """Bảng KPI: cùng kỳ, kỳ này, thay đổi YoY, quý này, quý trước, thay đổi QoQ.

    compute(where_sql) → dict {key: giá trị}; where_sql là điều kiện trên cột `purchase_month`.
    """
    k_prev, k_cur = compute(period.prev), compute(period.cur)
    k_pq, k_q = compute(period.pqtd), compute(period.qtd)
    rows = []
    for k in kpis:
        diff, diff_fmt = COMPARE[k.kind]
        rows.append({"Chỉ số": k.label,
                     period.label_prev: k.fmt.format(k_prev[k.key]), period.label_cur: k.fmt.format(k_cur[k.key]),
                     "Thay đổi YoY": diff_fmt.format(diff(k_prev[k.key], k_cur[k.key])),
                     f"Quý trước ({period.label_pq})": k.fmt.format(k_pq[k.key]),
                     f"Quý này ({period.label_q})": k.fmt.format(k_q[k.key]),
                     "Thay đổi QoQ": diff_fmt.format(diff(k_pq[k.key], k_q[k.key]))})
    return pd.DataFrame(rows).set_index("Chỉ số")
