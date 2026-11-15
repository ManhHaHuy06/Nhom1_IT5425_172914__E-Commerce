"""Quy tắc nghiệp vụ (giống Gold: olist-lakehouse/layers/gold/config.py), bảng màu và 5 vùng kinh tế Brazil."""

# ---------- Quy tắc nghiệp vụ ----------
LOST_STATUSES = ("canceled", "unavailable")   # đơn mất; đơn hợp lệ = không mất và có ≥ 1 dòng sản phẩm
REPORT_MONTH_MIN_SHARE = 0.10                 # tháng báo cáo = tháng mới nhất có số đơn hợp lệ ≥ 10% tháng cao nhất
MIN_SELLER_ORDERS = 30                        # người bán đủ đơn để đánh giá chất lượng (customer_experience §7, seller §5)
# Ngành của 1 dòng sản phẩm: tên tiếng Anh, thiếu thì tên gốc, thiếu cả hai thì 'unknown' (cần alias p = dim_product, cat = dim_category).
# Ngành chính của 1 đơn (customer_behaviour) chỉ dùng tên tiếng Anh, giống `main_category` của Gold.
CATEGORY_SQL = "coalesce(cat.product_category_name_english, p.product_category_name, 'unknown')"

# ---------- Bảng màu ----------
C_MAIN, C_BAD, C_GOOD, C_GREY, C_ACCENT = "#2563EB", "#DC2626", "#16A34A", "#9CA3AF", "#F59E0B"
C_ORANGE, C_YELLOW, C_MUTED_TEXT = "#F97316", "#FACC15", "#4B5563"
SEQ_BLUES = ["#DBEAFE", "#93C5FD", "#3B82F6", "#1D4ED8", "#1E3A8A"]      # dữ liệu có thứ bậc (nhạt → đậm)
CATEGORY_COLORS = ["#2563EB", "#F97316", "#16A34A", "#DC2626", "#A855F7", "#0891B2", "#F59E0B", "#DB2777"]

# ---------- 5 vùng kinh tế Brazil (IBGE): thứ tự và màu dùng chung ----------
REGION_OF = {
    "SP": "Sudeste", "RJ": "Sudeste", "MG": "Sudeste", "ES": "Sudeste",
    "PR": "Sul", "SC": "Sul", "RS": "Sul",
    "BA": "Nordeste", "PE": "Nordeste", "CE": "Nordeste", "MA": "Nordeste", "PB": "Nordeste",
    "RN": "Nordeste", "AL": "Nordeste", "PI": "Nordeste", "SE": "Nordeste",
    "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MT": "Centro-Oeste", "MS": "Centro-Oeste",
    "PA": "Norte", "AM": "Norte", "RO": "Norte", "TO": "Norte", "AC": "Norte", "AP": "Norte", "RR": "Norte",
}
REGIONS = ["Sudeste", "Sul", "Nordeste", "Centro-Oeste", "Norte"]
REGION_COLORS = {"Sudeste": "#2563EB", "Sul": "#16A34A", "Nordeste": C_ORANGE,
                 "Centro-Oeste": "#A855F7", "Norte": "#0891B2"}


def region_colors(regions=REGIONS) -> list:
    return [REGION_COLORS[r] for r in regions]
