"""Phần dùng chung cho 5 notebook EDA của dashboard Olist, chia theo module:

- `config`   — `GOLD` (olist-lakehouse/layers/gold/config.py: ngưỡng, band, phân khúc dùng chung với dashboard),
               quy tắc nghiệp vụ suy từ đó, bảng màu, 5 vùng;
- `period`   — `Period`: kỳ phân tích suy từ dữ liệu (cùng quy tắc với Gold / Power BI: YoY + QoQ), ghi đè bằng
               REPORT_START / REPORT_END; `Kpi`, `kpi_table` bảng KPI so sánh kỳ;
- `db`       — `TrinoSession`, `q`, `scalar`, `create_table_from_df` (kết nối hiện tại ở `db.con`), `reconcile`
               đối chiếu chỉ số với Gold, `gold_period_where`;
- `tables`   — `connect` (chép Silver → `EDA_SCHEMA`, tạo `state_ref`, `order_base`), `create_item_ps`, `create_order_payment`;
- `rfm`      — phân khúc RFM `customer_rfm`;
- `sql`      — `quantile_sql`, `median_sql`, `band_sql` (cú pháp Trino); `band_bins`, `band_edges`, `band_order`
               đổi band của Gold sang dạng notebook dùng;
- `plotting` — theme, hàm trục, chú thích vùng, cột nhóm, `band_labels`, `ordered`.

Bảng trung gian của EDA là bảng Iceberg trong schema riêng `iceberg.eda` (REST catalog không hỗ trợ view).
Notebook nào cần cấu hình riêng thì khai báo ngay trong notebook đó. Cách dùng:

    from common import *
    con, P = connect(["dim_product", "dim_category"])   # order_base + P được tạo sẵn

Cần lakehouse đang chạy (Trino `localhost:8085`) và đã chạy tới stage `silver`. Cấu hình qua biến môi trường:
TRINO_HOST, TRINO_PORT, TRINO_USER, TRINO_CATALOG, SILVER_SCHEMA, EDA_SCHEMA. Các notebook dùng chung tên bảng
trong `EDA_SCHEMA`, nên chạy lần lượt từng notebook (hoặc đặt EDA_SCHEMA khác nhau nếu chạy song song).
"""
from .config import (C_ACCENT, C_BAD, C_GOOD, C_GREY, C_MAIN, C_MUTED_TEXT, C_ORANGE, C_YELLOW, CATEGORY_COLORS,
                     CATEGORY_SQL, GOLD, MIN_SELLER_ORDERS, REGION_COLORS, REGIONS, SEQ_BLUES, STATE_POPULATION_2018,
                     region_colors)
from .db import gold_period_where, q, reconcile, scalar
from .period import Kpi, Period, kpi_table
from .plotting import (apply_theme, band_labels, brl_axis, grouped_bar, month_axis, ordered, pct_axis,
                       region_legend)
from .rfm import SEG_ORDER, create_customer_rfm
from .sql import band_bins, band_edges, band_order, band_sql, median_sql, quantile_sql
from .tables import connect, create_item_ps, create_order_payment

__all__ = [
    # bảng màu, vùng
    "C_MAIN", "C_BAD", "C_GOOD", "C_GREY", "C_ACCENT", "C_ORANGE", "C_YELLOW", "C_MUTED_TEXT",
    "SEQ_BLUES", "CATEGORY_COLORS", "REGIONS", "REGION_COLORS", "region_colors",
    # quy tắc nghiệp vụ, dữ liệu (connect trả về (con, Period); create_customer_rfm trả về Rfm)
    "GOLD", "STATE_POPULATION_2018", "MIN_SELLER_ORDERS", "CATEGORY_SQL", "connect", "q", "scalar", "create_item_ps", "create_order_payment", "create_customer_rfm", "SEG_ORDER",
    "Period", "reconcile", "gold_period_where",
    # SQL / pandas
    "band_sql", "quantile_sql", "median_sql", "band_labels", "ordered", "band_bins", "band_edges", "band_order",
    # biểu đồ
    "apply_theme", "pct_axis", "brl_axis", "month_axis", "region_legend", "grouped_bar",
    # KPI
    "Kpi", "kpi_table",
]
