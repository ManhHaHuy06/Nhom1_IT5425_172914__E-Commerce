"""Kỳ phân tích suy ra từ dữ liệu (cùng quy tắc với Gold `report_period` / Power BI), không cứng năm/tháng."""
from typing import NamedTuple


class Period(NamedTuple):
    """Kỳ so sánh suy ra từ dữ liệu: kỳ này = 01 → tháng báo cáo của năm mới nhất, cùng kỳ = các tháng đó năm trước.

    Cửa sổ xu hướng (biểu đồ theo tháng) = 01/năm trước → tháng báo cáo.
    """
    year_cur: int
    last_mon: int      # tháng báo cáo (1–12)

    @property
    def year_prev(self) -> int:
        return self.year_cur - 1

    @property
    def m_start(self) -> str:             # tháng đầu cửa sổ xu hướng (DATE)
        return f"{self.year_prev}-01-01"

    @property
    def m_end(self) -> str:               # tháng báo cáo (DATE, ngày 01)
        return f"{self.year_cur}-{self.last_mon:02d}-01"

    @property
    def cur_start(self) -> str:
        return f"{self.year_cur}-01-01"

    @property
    def cur(self) -> str:                 # điều kiện SQL: kỳ này (cần cột purchase_year, purchase_mon)
        return f"purchase_year = {self.year_cur} AND purchase_mon <= {self.last_mon}"

    @property
    def prev(self) -> str:                # điều kiện SQL: cùng kỳ năm trước
        return f"purchase_year = {self.year_prev} AND purchase_mon <= {self.last_mon}"

    @property
    def in_period(self) -> str:           # điều kiện SQL: cửa sổ xu hướng (cần cột purchase_month)
        return f"purchase_month BETWEEN DATE '{self.m_start}' AND DATE '{self.m_end}'"

    def label(self, year: int) -> str:    # "01–08/2018"
        return f"01–{self.last_mon:02d}/{year}"

    @property
    def label_cur(self) -> str:
        return self.label(self.year_cur)

    @property
    def label_prev(self) -> str:
        return self.label(self.year_prev)

    @property
    def label_trend(self) -> str:         # "01/2017–08/2018"
        return f"01/{self.year_prev}–{self.last_mon:02d}/{self.year_cur}"
