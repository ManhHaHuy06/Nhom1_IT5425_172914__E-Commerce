"""Kết nối Trino, chép bảng Silver sang EDA_SCHEMA và tạo các bảng trung gian dùng chung.

`order_base` (1 dòng / đơn, cờ `is_valid`), `item_ps` (dòng sản phẩm), `order_payment` (thanh toán theo đơn).
"""
import os

import pandas as pd
import trino

from . import db
from .config import CATEGORY_SQL, LOST_STATUSES, REGION_OF, REPORT_MONTH_MIN_SHARE
from .db import create_table_from_df, q, scalar
from .period import Period

BASE_TABLES = ["fact_orders", "fact_order_items", "fact_reviews", "dim_customer"]


def connect(tables: list = (), extra_state_cols: dict | None = None) -> tuple[db.TrinoSession, Period]:
    """Mở kết nối Trino, chép các bảng Silver cần dùng sang `EDA_SCHEMA`, tạo `state_ref`, `order_base`, suy ra kỳ phân tích.

    Bảng Silver được chép (CTAS) để có bản chụp nhất quán trong suốt notebook và để câu SQL trong notebook gọi tên bảng
    ngắn (`fact_orders`…). Cột TIMESTAMP WITH TIME ZONE của Spark được đổi về TIMESTAMP (UTC). `BASE_TABLES` luôn được chép.
    """
    catalog = os.getenv("TRINO_CATALOG", "iceberg")
    silver = os.getenv("SILVER_SCHEMA", "silver")
    eda = os.getenv("EDA_SCHEMA", "eda")
    conn = trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "8085")),
        user=os.getenv("TRINO_USER", "eda"),
        catalog=catalog,
        schema=eda,
        timezone="UTC",
    )
    db.con = con = db.TrinoSession(conn)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{eda}")
    for t in dict.fromkeys([*BASE_TABLES, *tables]):
        columns, _ = con.query(
            f"SELECT column_name, data_type FROM {catalog}.information_schema.columns "
            f"WHERE table_schema = '{silver}' AND table_name = '{t}' ORDER BY ordinal_position")
        if not columns:
            raise LookupError(f"Không thấy bảng {catalog}.{silver}.{t} (lakehouse đã chạy tới stage silver chưa?)")
        cols = [f"CAST({name} AT TIME ZONE 'UTC' AS TIMESTAMP(6)) AS {name}" if "with time zone" in dtype else name
                for name, dtype in columns]
        con.execute(f"CREATE OR REPLACE TABLE {t} AS SELECT {', '.join(cols)} FROM {catalog}.{silver}.{t}")

    state_ref = pd.DataFrame({"state": list(REGION_OF), "region": list(REGION_OF.values())})
    for col, values in (extra_state_cols or {}).items():
        state_ref[col] = state_ref.state.map(values)
    create_table_from_df("state_ref", state_ref)

    _create_order_base()
    period = _report_period()
    print(f"{period} | {scalar('SELECT count(*) FROM order_base'):,} đơn, "
          f"{scalar('SELECT sum(is_valid) FROM order_base'):,} đơn hợp lệ")
    return con, period


def _create_order_base():
    """`order_base`: 1 dòng / đơn (mọi trạng thái) với thời gian, vị trí khách, giỏ hàng, giao hàng và review.

    `is_valid` = trạng thái ∉ LOST_STATUSES **và** có ≥ 1 dòng sản phẩm (giống `is_valid` của Gold).
    Các cột giỏ hàng (n_items, gmv…) là NULL khi đơn không có sản phẩm.
    """
    lost = ", ".join(f"'{s}'" for s in LOST_STATUSES)
    db.con.execute(f"""
    CREATE OR REPLACE TABLE order_base AS
    WITH itm AS (
        SELECT order_key, count(*) AS n_items, count(DISTINCT product_key) AS n_products,
               count(DISTINCT seller_key) AS n_sellers, sum(price) AS product_value,
               sum(freight_value) AS freight_value, sum(price + freight_value) AS gmv
        FROM fact_order_items GROUP BY order_key),
    rev AS (
        SELECT order_key, avg(review_score) AS review_score,
               max(is_negative) AS is_negative,   -- có ít nhất 1 review 1–2★
               min(is_positive) AS is_positive    -- mọi review đều 4–5★
        FROM fact_reviews GROUP BY order_key)
    SELECT o.order_key, o.order_id, o.customer_key, c.customer_unique_id, o.order_status,
           o.order_purchase_timestamp,
           CAST(o.order_purchase_timestamp AS DATE)                      AS purchase_date,
           CAST(date_trunc('month', o.order_purchase_timestamp) AS DATE) AS purchase_month,
           year(o.order_purchase_timestamp)   AS purchase_year,
           month(o.order_purchase_timestamp)  AS purchase_mon,
           hour(o.order_purchase_timestamp)   AS purchase_hour,
           day_of_week(o.order_purchase_timestamp) AS dow,               -- ISO: 1 = Thứ 2 … 7 = Chủ nhật
           c.state AS customer_state, c.city AS customer_city, r.region AS customer_region, c.geolocation_key,
           CAST(o.order_status IN ({lost}) AS INTEGER) AS is_lost,
           CAST(o.order_status NOT IN ({lost}) AND itm.n_items IS NOT NULL AS INTEGER) AS is_valid,
           o.is_delivered, o.is_canceled, o.delivery_days, o.estimated_delivery_days, o.delivery_delay_days,
           CASE WHEN o.is_delivered = 1 AND o.delivery_delay_days IS NOT NULL
                THEN CAST(o.delivery_delay_days > 0 AS INTEGER) END AS is_late,  -- chỉ có giá trị khi đơn đã giao và đủ ngày
           itm.n_items, itm.n_products, itm.n_sellers, itm.product_value, itm.freight_value, itm.gmv,
           rev.review_score, rev.is_negative, rev.is_positive
    FROM fact_orders o
    LEFT JOIN dim_customer c ON c.customer_key = o.customer_key
    LEFT JOIN state_ref r ON r.state = c.state
    LEFT JOIN itm ON itm.order_key = o.order_key
    LEFT JOIN rev ON rev.order_key = o.order_key
    """)


def _report_period() -> Period:
    """Tháng báo cáo = tháng mới nhất có số đơn hợp lệ ≥ REPORT_MONTH_MIN_SHARE × tháng cao nhất (bỏ tháng cuối chưa đủ
    dữ liệu); kỳ này = 01 → tháng đó, trừ khi ghi đè bằng REPORT_START / REPORT_END (xem `Period.resolve`)."""
    monthly = q("SELECT purchase_month, count(*) AS n FROM order_base WHERE is_valid = 1 GROUP BY 1")
    usable = monthly[monthly.n >= REPORT_MONTH_MIN_SHARE * monthly.n.max()]
    return Period.resolve(usable.purchase_month.max())


def create_item_ps():
    """`item_ps`: 1 dòng / đơn vị sản phẩm trong đơn hợp lệ (dùng chung cho notebook product và seller).

    Cần đã nạp `dim_product`, `dim_category`, `dim_seller`. Ngành = tên tiếng Anh, thiếu thì tên gốc, thiếu cả hai thì `unknown`.
    """
    db.con.execute(f"""
    CREATE OR REPLACE TABLE item_ps AS
    SELECT i.item_key, i.order_key, i.product_key, p.product_id, i.seller_key, s.seller_id,
           o.purchase_month, o.purchase_year, o.purchase_mon,
           {CATEGORY_SQL} AS category,
           i.price, i.freight_value, i.price + i.freight_value AS gmv,
           s.state AS seller_state, sr.region AS seller_region,
           o.customer_unique_id, o.customer_state, o.customer_region
    FROM fact_order_items i
    JOIN order_base o ON o.order_key = i.order_key AND o.is_valid = 1
    LEFT JOIN dim_product p ON p.product_key = i.product_key
    LEFT JOIN dim_category cat ON cat.category_key = p.category_key
    LEFT JOIN dim_seller s ON s.seller_key = i.seller_key
    LEFT JOIN state_ref sr ON sr.state = s.state
    """)


def create_order_payment():
    """`order_payment`: 1 dòng / đơn có thanh toán. Hình thức chính = dòng thanh toán có giá trị lớn nhất
    (hoà thì theo `payment_sequential`); số kỳ trả góp = max. Cần đã nạp `fact_payments`."""
    db.con.execute("""
    CREATE OR REPLACE TABLE order_payment AS
    WITH ranked AS (
        SELECT *, row_number() OVER (PARTITION BY order_key ORDER BY payment_value DESC, payment_sequential) AS rn
        FROM fact_payments)
    SELECT order_key,
           sum(payment_value)                      AS payment_value,
           max(payment_installments)               AS installments,
           count(DISTINCT payment_type)            AS n_payment_types,
           max(CAST(payment_type = 'voucher' AS INTEGER)) AS used_voucher,
           max(payment_type) FILTER (WHERE rn = 1) AS payment_type
    FROM ranked GROUP BY order_key
    """)
