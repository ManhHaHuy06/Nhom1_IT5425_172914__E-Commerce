"""Quy tắc nghiệp vụ, bảng màu và 5 vùng kinh tế Brazil.

Ngưỡng / band / phân khúc dùng chung với tầng Gold **không khai báo lại ở đây**: chúng được đọc thẳng từ
`olist-lakehouse/layers/gold/config.py` (module `GOLD`), để EDA và dashboard Power BI luôn cùng một quy tắc.
Đường dẫn mặc định suy từ vị trí thư mục; đặt biến môi trường `OLIST_GOLD_CONFIG` nếu lakehouse nằm chỗ khác.
"""
import importlib.util
import os
from pathlib import Path

_GOLD_CONFIG = Path(os.getenv("OLIST_GOLD_CONFIG") or
                    Path(__file__).resolve().parents[4] / "olist-lakehouse" / "layers" / "gold" / "market" / "config.py")


def _load_gold_config():
    if not _GOLD_CONFIG.is_file():
        raise FileNotFoundError(f"Không thấy Gold config tại {_GOLD_CONFIG} (đặt OLIST_GOLD_CONFIG nếu lakehouse ở chỗ khác)")
    spec = importlib.util.spec_from_file_location("olist_gold_config", _GOLD_CONFIG)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


GOLD = _load_gold_config()   # vd. GOLD.ORDER_VALUE_BANDS, GOLD.REPURCHASE_WINDOW_DAYS, GOLD.SELLER_TIERS…

# ---------- Quy tắc nghiệp vụ (từ Gold) ----------
LOST_STATUSES = GOLD.INVALID_ORDER_STATUSES         # đơn mất; đơn hợp lệ = không mất và có ≥ 1 dòng sản phẩm
REPORT_MONTH_MIN_SHARE = GOLD.REPORT_MONTH_MIN_SHARE  # tháng báo cáo = tháng mới nhất có số đơn hợp lệ ≥ 10% tháng cao nhất
MIN_SELLER_ORDERS = GOLD.SELLER_MIN_ORDERS          # người bán đủ đơn để đánh giá chất lượng (05_seller §5–§6)
STATE_POPULATION_2018 = dict(GOLD.state_population())   # dân số ước tính 2018 theo bang (người), seed của Gold
# Ngành của 1 dòng sản phẩm: tên tiếng Anh, thiếu thì tên gốc, thiếu cả hai thì 'unknown' (cần alias p = dim_product, cat = dim_category).
# Ngành chính của 1 đơn (04_customer) chỉ dùng tên tiếng Anh, giống `main_category` của Gold.
CATEGORY_SQL = "coalesce(cat.product_category_name_english, p.product_category_name, 'unknown')"

# ---------- Bảng màu ----------
C_MAIN, C_BAD, C_GOOD, C_GREY, C_ACCENT = "#2563EB", "#DC2626", "#16A34A", "#9CA3AF", "#F59E0B"
C_ORANGE, C_YELLOW, C_MUTED_TEXT = "#F97316", "#FACC15", "#4B5563"
SEQ_BLUES = ["#DBEAFE", "#93C5FD", "#3B82F6", "#1D4ED8", "#1E3A8A"]      # dữ liệu có thứ bậc (nhạt → đậm)
CATEGORY_COLORS = ["#2563EB", "#F97316", "#16A34A", "#DC2626", "#A855F7", "#0891B2", "#F59E0B", "#DB2777"]

# ---------- 5 vùng kinh tế Brazil (IBGE): thứ tự (từ Gold) và màu dùng chung ----------
REGIONS = list(GOLD.REGION_ORDER)
REGION_OF = {state: region for state, region, _ in GOLD.REGION_ROWS}
REGION_COLORS = {"Sudeste": "#2563EB", "Sul": "#16A34A", "Nordeste": C_ORANGE,
                 "Centro-Oeste": "#A855F7", "Norte": "#0891B2"}


def region_colors(regions=REGIONS) -> list:
    return [REGION_COLORS[r] for r in regions]
