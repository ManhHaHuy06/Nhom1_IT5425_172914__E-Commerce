-- ClickHouse serving schema cho nhánh Gold Seller (27 bảng phục vụ serving/seller-dashboard).
-- Chạy lại mỗi lần stage clickhouse-seller: CREATE OR REPLACE chỉ tạo lại bảng tạm <t>__new,
-- bảng phục vụ <t> giữ nguyên dữ liệu cho tới khi load_seller.sql chạy EXCHANGE TABLES.
-- Thứ tự cột khớp 1-1 với layers/gold/seller/create_gold_tables.sql (positional INSERT ... SELECT *).

CREATE DATABASE IF NOT EXISTS olist_gold_seller;

-- ============================================================================ staging tables (*__new)
CREATE OR REPLACE TABLE olist_gold_seller.sales_detail__new (
    item_key                       Int64,
    order_key                      Int64,
    order_id                       String,
    order_item_id                  Int32,
    product_key                    Int64,
    seller_key                     Int64,
    date_key                       Int32,
    shipping_limit_date            Nullable(DateTime64(6)),
    price                          Nullable(Float64),
    freight_value                  Nullable(Float64),
    item_total                     Float64,
    customer_key                   Int64,
    purchase_date_key              Int32,
    order_status                   LowCardinality(String),
    order_purchase_timestamp       DateTime64(6),
    order_approved_at              Nullable(DateTime64(6)),
    order_delivered_carrier_date   Nullable(DateTime64(6)),
    order_delivered_customer_date  Nullable(DateTime64(6)),
    order_estimated_delivery_date  Nullable(DateTime64(6)),
    delivery_days                  Nullable(Int32),
    estimated_delivery_days        Nullable(Int32),
    delivery_delay_days            Nullable(Int32),
    is_delivered                   Int32,
    is_canceled                    Int32,
    purchase_date                  Date,
    month                          Date,
    purchase_hour                  Int32,
    weekday_num                    Int32,
    seller_id                      String,
    seller_city                    Nullable(String),
    seller_state                   LowCardinality(String),
    product_id                     String,
    category_key                   Nullable(Int64),
    product_category_name          LowCardinality(Nullable(String)),
    product_category_name_english  LowCardinality(String),
    customer_unique_id             Nullable(String),
    customer_city                  Nullable(String),
    customer_state                 LowCardinality(String),
    latitude                       Nullable(Float64),
    longitude                      Nullable(Float64),
    revenue                        Float64,
    review_score                   Nullable(Int32),
    is_positive                    Nullable(Int32),
    is_negative                    Nullable(Int32),
    review_comment_title           Nullable(String),
    review_comment_message         Nullable(String),
    review_creation_date           Nullable(DateTime64(6)),
    review_answer_timestamp        Nullable(DateTime64(6))
) ENGINE = MergeTree
ORDER BY (seller_id, purchase_date, order_id, order_item_id);

CREATE OR REPLACE TABLE olist_gold_seller.orders__new (
    order_key                      Int64,
    order_id                       String,
    customer_key                   Int64,
    purchase_date_key              Int32,
    order_status                   LowCardinality(String),
    order_purchase_timestamp       DateTime64(6),
    order_approved_at              Nullable(DateTime64(6)),
    order_delivered_carrier_date   Nullable(DateTime64(6)),
    order_delivered_customer_date  Nullable(DateTime64(6)),
    order_estimated_delivery_date  Nullable(DateTime64(6)),
    delivery_days                  Nullable(Int32),
    estimated_delivery_days        Nullable(Int32),
    delivery_delay_days            Nullable(Int32),
    is_delivered                   Int32,
    is_canceled                    Int32,
    purchase_date                  Date,
    month                          Date,
    purchase_hour                  Int32,
    weekday_num                    Int32,
    payment_value                  Nullable(Float64),
    payment_transaction_count      Nullable(Int64),
    payment_type_count             Nullable(Int64),
    review_score                   Nullable(Int32),
    is_positive                    Nullable(Int32),
    is_negative                    Nullable(Int32),
    review_creation_date           Nullable(DateTime64(6))
) ENGINE = MergeTree
ORDER BY (purchase_date, order_id);

CREATE OR REPLACE TABLE olist_gold_seller.payments_by_order__new (
    order_id                       String,
    payment_value                  Nullable(Float64),
    payment_transaction_count      Int64,
    payment_type_count             Int64
) ENGINE = MergeTree
ORDER BY order_id;

CREATE OR REPLACE TABLE olist_gold_seller.reviews__new (
    review_key                     Int64,
    order_key                      Int64,
    order_id                       String,
    review_id                      String,
    review_score                   Nullable(Int32),
    review_comment_title           Nullable(String),
    review_comment_message         Nullable(String),
    review_creation_date           Nullable(DateTime64(6)),
    review_answer_timestamp        Nullable(DateTime64(6)),
    is_positive                    Nullable(Int32),
    is_negative                    Nullable(Int32)
) ENGINE = MergeTree
ORDER BY (order_id, review_id);

CREATE OR REPLACE TABLE olist_gold_seller.seller_order__new (
    seller_id                      String,
    order_id                       String,
    order_key                      Int64,
    customer_key                   Int64,
    customer_unique_id             Nullable(String),
    customer_state                 LowCardinality(String),
    order_purchase_timestamp       DateTime64(6),
    purchase_date                  Date,
    month                          Date,
    purchase_hour                  Int32,
    weekday_num                    Int32,
    order_status                   LowCardinality(String),
    is_delivered                   Nullable(Int64),
    is_canceled                    Nullable(Int64),
    delivery_days                  Nullable(Int32),
    estimated_delivery_days        Nullable(Int32),
    delivery_delay_days            Nullable(Int32),
    seller_state                   LowCardinality(String),
    revenue                        Nullable(Float64),
    item_count                     Int64
) ENGINE = MergeTree
ORDER BY (seller_id, purchase_date, order_id);

CREATE OR REPLACE TABLE olist_gold_seller.customer_order_bridge__new (
    seller_id                      String,
    customer_unique_id             Nullable(String),
    order_id                       String,
    purchase_date                  Date,
    month                          Date,
    revenue                        Nullable(Float64),
    item_count                     Int64
) ENGINE = MergeTree
ORDER BY (seller_id, purchase_date, order_id);

CREATE OR REPLACE TABLE olist_gold_seller.sales_performance_daily__new (
    seller_id                      String,
    purchase_date                  Date,
    order_count                    Int64,
    item_count                     Int64,
    total_revenue                  Nullable(Float64),
    total_freight                  Nullable(Float64),
    avg_freight                    Nullable(Float64)
) ENGINE = MergeTree
ORDER BY (seller_id, purchase_date);

CREATE OR REPLACE TABLE olist_gold_seller.sales_performance_monthly__new (
    seller_id                      String,
    month                          Date,
    order_count                    Int64,
    item_count                     Int64,
    total_revenue                  Nullable(Float64),
    total_freight                  Nullable(Float64),
    avg_freight                    Nullable(Float64),
    avg_item_price                 Nullable(Float64)
) ENGINE = MergeTree
ORDER BY (seller_id, month);

CREATE OR REPLACE TABLE olist_gold_seller.category_performance_daily__new (
    seller_id                      String,
    purchase_date                  Date,
    product_category_name_english  LowCardinality(String),
    order_count                    Int64,
    item_count                     Int64,
    total_revenue                  Nullable(Float64)
) ENGINE = MergeTree
ORDER BY (seller_id, purchase_date, product_category_name_english);

CREATE OR REPLACE TABLE olist_gold_seller.category_performance_monthly__new (
    seller_id                      String,
    month                          Date,
    product_category_name_english  LowCardinality(String),
    order_count                    Int64,
    item_count                     Int64,
    total_revenue                  Nullable(Float64)
) ENGINE = MergeTree
ORDER BY (seller_id, month, product_category_name_english);

CREATE OR REPLACE TABLE olist_gold_seller.product_performance_monthly__new (
    seller_id                      String,
    month                          Date,
    product_key                    Int64,
    product_id                     String,
    product_category_name_english  LowCardinality(String),
    order_count                    Int64,
    item_count                     Int64,
    total_revenue                  Nullable(Float64),
    avg_item_price                 Nullable(Float64)
) ENGINE = MergeTree
ORDER BY (seller_id, month, product_id);

CREATE OR REPLACE TABLE olist_gold_seller.geography_performance_daily__new (
    seller_id                      String,
    purchase_date                  Date,
    customer_state                 LowCardinality(String),
    order_count                    Int64,
    customer_count                 Int64,
    total_revenue                  Nullable(Float64)
) ENGINE = MergeTree
ORDER BY (seller_id, purchase_date, customer_state);

CREATE OR REPLACE TABLE olist_gold_seller.geography_performance_monthly__new (
    seller_id                      String,
    month                          Date,
    customer_state                 LowCardinality(String),
    order_count                    Int64,
    customer_count                 Int64,
    total_revenue                  Nullable(Float64)
) ENGINE = MergeTree
ORDER BY (seller_id, month, customer_state);

CREATE OR REPLACE TABLE olist_gold_seller.order_status_daily__new (
    seller_id                      String,
    purchase_date                  Date,
    order_status                   LowCardinality(String),
    order_count                    Int64
) ENGINE = MergeTree
ORDER BY (seller_id, purchase_date, order_status);

CREATE OR REPLACE TABLE olist_gold_seller.order_status_monthly__new (
    seller_id                      String,
    month                          Date,
    order_status                   LowCardinality(String),
    order_count                    Int64
) ENGINE = MergeTree
ORDER BY (seller_id, month, order_status);

CREATE OR REPLACE TABLE olist_gold_seller.delivery_performance_daily__new (
    seller_id                      String,
    purchase_date                  Date,
    order_count                    Int64,
    delivered_orders               Nullable(Int64),
    canceled_orders                Nullable(Int64),
    late_orders                    Nullable(Int64),
    delivery_days_sum              Nullable(Float64),
    delivery_days_count            Int64,
    delay_days_sum                 Nullable(Float64),
    delay_days_count               Int64,
    late_rate                      Nullable(Float64)
) ENGINE = MergeTree
ORDER BY (seller_id, purchase_date);

CREATE OR REPLACE TABLE olist_gold_seller.delivery_performance_monthly__new (
    seller_id                      String,
    month                          Date,
    order_count                    Int64,
    delivered_orders               Nullable(Int64),
    canceled_orders                Nullable(Int64),
    late_orders                    Nullable(Int64),
    delivery_days_sum              Nullable(Float64),
    delivery_days_count            Int64,
    delay_days_sum                 Nullable(Float64),
    delay_days_count               Int64,
    late_rate                      Nullable(Float64)
) ENGINE = MergeTree
ORDER BY (seller_id, month);

CREATE OR REPLACE TABLE olist_gold_seller.order_hourly_daily__new (
    seller_id                      String,
    purchase_date                  Date,
    purchase_hour                  Int32,
    order_count                    Int64
) ENGINE = MergeTree
ORDER BY (seller_id, purchase_date, purchase_hour);

CREATE OR REPLACE TABLE olist_gold_seller.order_hourly_monthly__new (
    seller_id                      String,
    month                          Date,
    purchase_hour                  Int32,
    order_count                    Int64
) ENGINE = MergeTree
ORDER BY (seller_id, month, purchase_hour);

CREATE OR REPLACE TABLE olist_gold_seller.order_weekday_monthly__new (
    seller_id                      String,
    month                          Date,
    weekday_num                    Int32,
    order_count                    Int64
) ENGINE = MergeTree
ORDER BY (seller_id, month, weekday_num);

CREATE OR REPLACE TABLE olist_gold_seller.review_score_daily__new (
    seller_id                      String,
    purchase_date                  Date,
    review_score                   Nullable(Int32),
    reviewed_orders                Int64
) ENGINE = MergeTree
ORDER BY (seller_id, purchase_date);

CREATE OR REPLACE TABLE olist_gold_seller.review_score_monthly__new (
    seller_id                      String,
    month                          Date,
    review_score                   Nullable(Int32),
    reviewed_orders                Int64
) ENGINE = MergeTree
ORDER BY (seller_id, month);

CREATE OR REPLACE TABLE olist_gold_seller.review_geography_monthly__new (
    seller_id                      String,
    month                          Date,
    customer_state                 LowCardinality(String),
    review_score                   Nullable(Int32),
    reviewed_orders                Int64
) ENGINE = MergeTree
ORDER BY (seller_id, month, customer_state);

CREATE OR REPLACE TABLE olist_gold_seller.review_category_monthly__new (
    seller_id                      String,
    month                          Date,
    product_category_name_english  LowCardinality(String),
    review_score                   Nullable(Int32),
    reviewed_orders                Int64
) ENGINE = MergeTree
ORDER BY (seller_id, month, product_category_name_english);

CREATE OR REPLACE TABLE olist_gold_seller.customer_lifetime__new (
    seller_id                      String,
    customer_unique_id             Nullable(String),
    order_count                    Int64,
    item_count                     Int64,
    total_revenue                  Nullable(Float64),
    first_purchase_date            Nullable(Date),
    last_purchase_date             Nullable(Date)
) ENGINE = MergeTree
ORDER BY seller_id;

CREATE OR REPLACE TABLE olist_gold_seller.customer_activity_monthly__new (
    seller_id                      String,
    month                          Date,
    customer_unique_id             String,
    order_count                    Int64,
    total_revenue                  Nullable(Float64),
    customer_state                 LowCardinality(Nullable(String))
) ENGINE = MergeTree
ORDER BY (seller_id, month, customer_unique_id);

CREATE OR REPLACE TABLE olist_gold_seller.customer_activity_daily__new (
    seller_id                      String,
    purchase_date                  Date,
    customer_unique_id             String,
    order_count                    Int64,
    total_revenue                  Nullable(Float64)
) ENGINE = MergeTree
ORDER BY (seller_id, purchase_date, customer_unique_id);

-- ============================================================================ serving tables
CREATE TABLE IF NOT EXISTS olist_gold_seller.sales_detail AS olist_gold_seller.sales_detail__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.orders AS olist_gold_seller.orders__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.payments_by_order AS olist_gold_seller.payments_by_order__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.reviews AS olist_gold_seller.reviews__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.seller_order AS olist_gold_seller.seller_order__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.customer_order_bridge AS olist_gold_seller.customer_order_bridge__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.sales_performance_daily AS olist_gold_seller.sales_performance_daily__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.sales_performance_monthly AS olist_gold_seller.sales_performance_monthly__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.category_performance_daily AS olist_gold_seller.category_performance_daily__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.category_performance_monthly AS olist_gold_seller.category_performance_monthly__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.product_performance_monthly AS olist_gold_seller.product_performance_monthly__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.geography_performance_daily AS olist_gold_seller.geography_performance_daily__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.geography_performance_monthly AS olist_gold_seller.geography_performance_monthly__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.order_status_daily AS olist_gold_seller.order_status_daily__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.order_status_monthly AS olist_gold_seller.order_status_monthly__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.delivery_performance_daily AS olist_gold_seller.delivery_performance_daily__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.delivery_performance_monthly AS olist_gold_seller.delivery_performance_monthly__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.order_hourly_daily AS olist_gold_seller.order_hourly_daily__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.order_hourly_monthly AS olist_gold_seller.order_hourly_monthly__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.order_weekday_monthly AS olist_gold_seller.order_weekday_monthly__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.review_score_daily AS olist_gold_seller.review_score_daily__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.review_score_monthly AS olist_gold_seller.review_score_monthly__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.review_geography_monthly AS olist_gold_seller.review_geography_monthly__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.review_category_monthly AS olist_gold_seller.review_category_monthly__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.customer_lifetime AS olist_gold_seller.customer_lifetime__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.customer_activity_monthly AS olist_gold_seller.customer_activity_monthly__new;
CREATE TABLE IF NOT EXISTS olist_gold_seller.customer_activity_daily AS olist_gold_seller.customer_activity_daily__new;
