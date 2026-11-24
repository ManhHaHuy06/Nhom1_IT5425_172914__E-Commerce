"""Ngưỡng và bảng tra cứu cho tầng Gold (khớp build_data*.py và các notebook EDA của dashboard Power BI)."""

from __future__ import annotations

import csv
from pathlib import Path

SEEDS_DIR = Path(__file__).resolve().parent / "seeds"

# ---------- Kỳ báo cáo ----------
# Tháng báo cáo = tháng mới nhất có số đơn hợp lệ >= 10% tháng cao nhất (bỏ tháng dữ liệu còn dở dang),
# giống cột dim_date[Report Key] trong model Power BI.
REPORT_MONTH_MIN_SHARE = 0.10

# ---------- Vùng kinh tế (IBGE) ----------
REGION_ORDER = ["Sudeste", "Sul", "Nordeste", "Centro-Oeste", "Norte"]
REGION_STATES = {
    "Sudeste": ["ES", "MG", "RJ", "SP"],
    "Sul": ["PR", "RS", "SC"],
    "Nordeste": ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"],
    "Centro-Oeste": ["DF", "GO", "MS", "MT"],
    "Norte": ["AC", "AM", "AP", "PA", "RO", "RR", "TO"],
}
REGION_ROWS = [
    (state, region, REGION_ORDER.index(region) + 1)
    for region, states in REGION_STATES.items()
    for state in states
]

INVALID_ORDER_STATUSES = ("canceled", "unavailable")

# ---------- Ngành hàng (product.ipynb) ----------
MIN_CATEGORY_SHARE = 0.005   # tỷ trọng tối thiểu để vào ma trận tăng trưởng ngành
TOP_CATEGORY_N = 8           # số ngành riêng trong category_group, còn lại là "Khác"
TOP_FLOW_CATEGORY_N = 12     # is_top12_category của item_flow

# ---------- Bang (geo.ipynb) ----------
STATE_MATRIX_MIN_PY_ORDERS = 100  # bang cần >= 100 đơn ở kỳ cùng kỳ năm trước để vào ma trận cơ hội

# ---------- Người bán (seller.ipynb) ----------
SELLER_MIN_ORDERS = 30                   # tối thiểu để xếp tầng điểm review
ACTIVE_FROM_MONTH, ACTIVE_TO_MONTH = 6, 8  # còn bán trong tháng 6–8 kể từ tháng đầu

# ---------- Khách hàng ----------
REPURCHASE_WINDOW_DAYS = 90

# ---------- Band: (cận trên không gồm, nhãn); cận trên None = phần còn lại ----------
ORDER_VALUE_BANDS = [(50, "<50"), (100, "50-100"), (150, "100-150"), (200, "150-200"),
                     (300, "200-300"), (500, "300-500"), (1000, "500-1000"), (None, ">1000")]
PRICE_BANDS = [(50, "< 50"), (100, "50–100"), (200, "100–200"), (500, "200–500"),
               (1000, "500–1.000"), (None, "≥ 1.000")]
SELLER_TIERS = [(1000, "Siêu nhỏ (< 1K)"), (5000, "Nhỏ (1K–5K)"), (20000, "Vừa (5K–20K)"),
                (100000, "Lớn (20K–100K)"), (None, "Chiến lược (≥ 100K)")]
SELLER_REVIEW_BANDS = [(3.5, "< 3.5"), (3.8, "3.5–3.8"), (4.0, "3.8–4.0"), (4.3, "4.0–4.3"), (None, "≥ 4.3")]
SELLER_REVIEW_BAND_FEW_ORDERS = "< 30 đơn"

# Band theo cận trên GỒM (<=)
SKU_UNIT_BANDS = [(1, "1"), (5, "2–5"), (20, "6–20"), (100, "21–100"), (None, "> 100")]
CATEGORY_COUNT_BANDS = [(1, "1 ngành"), (2, "2 ngành"), (4, "3–4 ngành"), (None, "≥ 5 ngành")]
GAP_BANDS = [(0, "Cùng ngày"), (7, "1-7 ngày"), (30, "8-30 ngày"), (90, "31-90 ngày"),
             (180, "91-180 ngày"), (365, "181-365 ngày"), (None, ">365 ngày")]
DELAY_BANDS = [(-10, "Sớm ≥10 ngày"), (-5, "Sớm 5–9 ngày"), (0, "Sớm 0–4 ngày / đúng hạn"),
               (3, "Trễ 1–3 ngày"), (7, "Trễ 4–7 ngày"), (14, "Trễ 8–14 ngày"), (None, "Trễ >14 ngày")]

MAX_INSTALLMENT_BAND = 10  # 10+ kỳ gộp một nhóm

# ---------- RFM (executive_overview / customer_behaviour) ----------
RFM_QUANTILES = [0.2, 0.4, 0.6, 0.8]
RFM_FREQUENCY_CUTS = [1, 2, 3, 4]  # F = 1..5 theo số đơn 1|2|3|4|5+
# (tên phân khúc, điều kiện SQL trên R/F/M) — xét theo thứ tự, không khớp = "Đã rời bỏ"
RFM_SEGMENTS = [
    ("Champions", "F >= 2 AND R >= 4"),
    ("Loyal - cần giữ chân", "F >= 2"),
    ("Khách mới", "R >= 4"),
    ("Cần nuôi dưỡng", "R = 3"),
    ("Giá trị cao sắp mất", "M >= 4"),
]
RFM_OTHER_SEGMENT = "Đã rời bỏ"


def state_population() -> list[tuple[str, int]]:
    """Dân số ước tính 2018 theo bang (seed tĩnh, IBGE, làm tròn)."""
    with open(SEEDS_DIR / "state_population.csv", encoding="utf-8") as handle:
        return [(row["state"], int(row["population_2018"])) for row in csv.DictReader(handle)]
