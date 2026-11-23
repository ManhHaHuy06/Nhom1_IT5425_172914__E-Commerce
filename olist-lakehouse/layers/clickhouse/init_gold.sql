-- ClickHouse serving schema cho tầng Gold (docs/GOLD_DESIGN.md §4).
-- Chạy lại mỗi lần stage `clickhouse`: CREATE OR REPLACE chỉ tạo lại bảng tạm <t>__new theo DDL Iceberg mới nhất,
-- bảng phục vụ <t> giữ nguyên dữ liệu. load_gold.sql nạp vào <t>__new rồi EXCHANGE sang <t>.
-- Thứ tự cột của bảng phải trùng layers/gold/create_gold_tables.sql (INSERT ... SELECT * FROM s3() map theo vị trí).

CREATE DATABASE IF NOT EXISTS olist_gold;

-- Mart mẫu cũ, không visual nào dùng.
DROP TABLE IF EXISTS olist_gold.sales_performance_daily;
DROP TABLE IF EXISTS olist_gold.delivery_delay_analysis;
DROP TABLE IF EXISTS olist_gold.seller_scorecard;

-- ============================================================================ bảng vật lý
CREATE OR REPLACE TABLE olist_gold.report_period__new (
    report_year                 Int32,
    report_month                Int32,
    cy_start                    Date,
    cy_end                      Date,
    py_start                    Date,
    py_end                      Date,
    trend_start                 Date,
    label_cy                    String,
    label_py                    String,
    market_growth               Nullable(Float64),
    category_share_median       Nullable(Float64),
    state_penetration_median    Nullable(Float64),
    state_growth_median         Nullable(Float64),
    built_at                    DateTime64(6)
) ENGINE = MergeTree
ORDER BY tuple();

CREATE OR REPLACE TABLE olist_gold.fact_order_summary__new (
    order_key                   Int64,
    order_id                    String,
    customer_key                Int64,
    order_status                LowCardinality(String),
    state                       LowCardinality(Nullable(String)),
    region                      LowCardinality(Nullable(String)),
    region_order                Nullable(Int32),
    customer_city_key           Nullable(String),
    purchase_date               Date,
    purchase_hour               Int32,
    n_items                     Nullable(Int32),
    n_products                  Nullable(Int32),
    n_sellers                   Nullable(Int32),
    product_value               Nullable(Float64),
    freight_value               Nullable(Float64),
    order_gmv                   Nullable(Float64),
    main_category               LowCardinality(Nullable(String)),
    payment_type                LowCardinality(String),
    installments                Nullable(Int32),
    is_valid                    Int32,
    order_value_band            LowCardinality(Nullable(String)),
    band_order                  Nullable(Int32),
    inst_order                  Nullable(Int32),
    installment_band            LowCardinality(Nullable(String)),
    is_multi_product            Nullable(Int32),
    is_delivered                Int32,
    delivery_days               Nullable(Int32),
    estimated_delivery_days     Nullable(Int32),
    delivery_delay_days         Nullable(Int32),
    review_score                Nullable(Float64),
    is_negative                 Nullable(Int32),
    is_positive                 Nullable(Int32),
    is_late                     Nullable(Int32),
    score_int                   Nullable(Int32),
    score_label                 LowCardinality(Nullable(String)),
    delay_bucket                LowCardinality(Nullable(String)),
    delay_bucket_order          Nullable(Int32),
    delivery_status             LowCardinality(Nullable(String)),
    delivery_days_capped        Nullable(Int32),
    items_band                  LowCardinality(Nullable(String)),
    items_band_order            Nullable(Int32),
    sellers_band                LowCardinality(Nullable(String)),
    sellers_band_order          Nullable(Int32)
) ENGINE = MergeTree
ORDER BY (purchase_date, order_id);

CREATE OR REPLACE TABLE olist_gold.fact_item_sales__new (
    order_key                   Int64,
    order_id                    String,
    order_item_id               Int32,
    product_key                 Int64,
    product_id                  Nullable(String),
    seller_key                  Int64,
    seller_id                   Nullable(String),
    customer_key                Int64,
    purchase_date               Date,
    category                    LowCardinality(Nullable(String)),
    price                       Nullable(Float64),
    freight_value               Nullable(Float64),
    item_gmv                    Nullable(Float64),
    price_band                  LowCardinality(Nullable(String)),
    price_band_order            Nullable(Int32),
    sku_band                    LowCardinality(Nullable(String)),
    sku_band_order              Nullable(Int32),
    customer_state              LowCardinality(Nullable(String)),
    seller_state                LowCardinality(Nullable(String)),
    seller_region               LowCardinality(Nullable(String)),
    seller_region_order         Nullable(Int32),
    seller_city_key             Nullable(String),
    is_local_seller             Nullable(Int32),
    is_top12_category           LowCardinality(String)
) ENGINE = MergeTree
ORDER BY (purchase_date, order_id, order_item_id);

CREATE OR REPLACE TABLE olist_gold.dim_date__new (
    date                        Date,
    year                        Int32,
    month                       Int32,
    year_month                  String,
    year_month_order            Int32,
    dow                         Int32,
    weekday                     LowCardinality(String)
) ENGINE = MergeTree
ORDER BY date;

CREATE OR REPLACE TABLE olist_gold.dim_state__new (
    state                       String,
    region                      LowCardinality(String),
    region_order                Int32,
    population_2018             Nullable(Int64),
    in_matrix                   LowCardinality(String),
    quadrant                    LowCardinality(String),
    quadrant_order              Int32
) ENGINE = MergeTree
ORDER BY state;

CREATE OR REPLACE TABLE olist_gold.dim_category__new (
    category                    String,
    category_group              String,
    category_group_order        Int32,
    in_matrix                   LowCardinality(String),
    quadrant                    LowCardinality(String),
    quadrant_order              Int32
) ENGINE = MergeTree
ORDER BY category;

CREATE OR REPLACE TABLE olist_gold.dim_seller__new (
    seller_key                  Int64,
    seller_id                   Nullable(String),
    seller_state                LowCardinality(Nullable(String)),
    seller_city_key             Nullable(String),
    seller_region               LowCardinality(Nullable(String)),
    seller_region_order         Nullable(Int32),
    gmv                         Nullable(Float64),
    orders                      Int64,
    gmv_rank                    Int32,
    seller_tier                 LowCardinality(Nullable(String)),
    seller_tier_order           Nullable(Int32),
    first_month                 Date,
    first_year                  Int32,
    seller_cohort               LowCardinality(String),
    seller_cohort_order         Int32,
    retention_observable        LowCardinality(String),
    active_m6_8                 Nullable(Int32),
    avg_review                  Nullable(Float64),
    negative_rate               Nullable(Float64),
    late_rate                   Nullable(Float64),
    review_band                 LowCardinality(String),
    review_band_order           Int32,
    n_categories                Int32,
    category_band               LowCardinality(String),
    category_band_order         Int32
) ENGINE = MergeTree
ORDER BY seller_key;

CREATE OR REPLACE TABLE olist_gold.dim_customer_profile__new (
    customer_key                Int64,
    customer_unique_id          Nullable(String),
    state                       LowCardinality(Nullable(String)),
    region                      LowCardinality(Nullable(String)),
    recency_days                Nullable(Int32),
    frequency                   Nullable(Int32),
    monetary                    Nullable(Float64),
    R                           Nullable(Int32),
    F                           Nullable(Int32),
    M                           Nullable(Int32),
    segment                     LowCardinality(Nullable(String)),
    segment_order               Nullable(Int32),
    days_to_2nd                 Nullable(Int32),
    gap_bucket                  LowCardinality(Nullable(String)),
    gap_order                   Nullable(Int32),
    first_purchase_date         Nullable(Date),
    first_score_int             Nullable(Int32),
    first_score_label           LowCardinality(Nullable(String)),
    repurchase_90d              Nullable(Int32),
    repurchase_observable       Nullable(Int32)
) ENGINE = MergeTree
ORDER BY customer_key;

-- Dự đoán ML (stage predict). Cùng thứ tự cột với layers/gold/create_gold_tables.sql.
CREATE OR REPLACE TABLE olist_gold.order_predictions__new (
    order_id            String,
    order_key           Nullable(Int64),
    purchase_date       Date,
    late_risk           Float64,
    expected_days       Float64,
    safe_promise_days   Float64,
    is_flagged          Int32,
    is_escalated        Int32,
    model_version       String,
    scored_at           DateTime64(6)
) ENGINE = MergeTree
ORDER BY (purchase_date, order_id);

CREATE OR REPLACE TABLE olist_gold.forecast_daily_orders__new (
    level           String,
    origin_date     Date,
    target_date     Date,
    horizon_day     Int32,
    forecast        Float64,
    lo80            Float64,
    hi80            Float64,
    model_version   String,
    scored_at       DateTime64(6)
) ENGINE = MergeTree
ORDER BY (level, target_date);

-- Bảng phục vụ: lần đầu tạo rỗng theo schema <t>__new, các lần sau giữ nguyên (EXCHANGE trong load_gold.sql đưa schema mới sang).
-- shortcut: view tham chiếu cột mới thêm vào DDL sẽ lỗi khi bảng phục vụ còn schema cũ; khi đó DROP TABLE olist_gold.<t> một lần rồi chạy lại stage clickhouse.
CREATE TABLE IF NOT EXISTS olist_gold.report_period AS olist_gold.report_period__new;
CREATE TABLE IF NOT EXISTS olist_gold.fact_order_summary AS olist_gold.fact_order_summary__new;
CREATE TABLE IF NOT EXISTS olist_gold.fact_item_sales AS olist_gold.fact_item_sales__new;
CREATE TABLE IF NOT EXISTS olist_gold.dim_date AS olist_gold.dim_date__new;
CREATE TABLE IF NOT EXISTS olist_gold.dim_state AS olist_gold.dim_state__new;
CREATE TABLE IF NOT EXISTS olist_gold.dim_category AS olist_gold.dim_category__new;
CREATE TABLE IF NOT EXISTS olist_gold.dim_seller AS olist_gold.dim_seller__new;
CREATE TABLE IF NOT EXISTS olist_gold.dim_customer_profile AS olist_gold.dim_customer_profile__new;
CREATE TABLE IF NOT EXISTS olist_gold.order_predictions AS olist_gold.order_predictions__new;
CREATE TABLE IF NOT EXISTS olist_gold.forecast_daily_orders AS olist_gold.forecast_daily_orders__new;

-- ============================================================================ view cho Power BI
-- Tên view = tên bảng trong model, cột = header CSV cũ trong olist-eda/powerbi/data/
-- → Power BI chỉ đổi bước Source của M query (customer_key đổi từ text sang số nguyên).

CREATE OR REPLACE VIEW olist_gold.order_summary AS
SELECT order_id, is_delivered, delivery_days, estimated_delivery_days, delivery_delay_days,
       review_score, is_negative, is_positive, customer_key, order_status, state, region, region_order,
       purchase_date, purchase_hour, n_items, n_products, n_sellers, product_value, freight_value,
       order_gmv AS gmv, main_category, payment_type, installments, is_valid, order_value_band, band_order,
       inst_order, installment_band, is_multi_product, is_late, score_int, score_label, delay_bucket,
       delay_bucket_order, delivery_status, delivery_days_capped, items_band, items_band_order,
       sellers_band, sellers_band_order
FROM olist_gold.fact_order_summary;

CREATE OR REPLACE VIEW olist_gold.item_sales AS
SELECT order_id, order_item_id, product_id, seller_id, category, price, freight_value, item_gmv,
       price_band, price_band_order, sku_band, sku_band_order
FROM olist_gold.fact_item_sales;

CREATE OR REPLACE VIEW olist_gold.item_flow AS
SELECT order_id, seller_state, seller_region, seller_region_order, category, item_gmv,
       is_local_seller, is_top12_category
FROM olist_gold.fact_item_sales;

CREATE OR REPLACE VIEW olist_gold.geo_customer AS
SELECT order_id, state AS customer_state,
       substring(customer_city_key, position(customer_city_key, '|') + 1) AS customer_city,
       customer_city_key
FROM olist_gold.fact_order_summary;

CREATE OR REPLACE VIEW olist_gold.geo_seller AS
SELECT DISTINCT order_id, seller_id, seller_state,
       substring(seller_city_key, position(seller_city_key, '|') + 1) AS seller_city,
       seller_city_key
FROM olist_gold.fact_item_sales;

CREATE OR REPLACE VIEW olist_gold.customer_rfm AS
SELECT customer_key, recency_days, frequency, monetary, R, F, M, segment, segment_order
FROM olist_gold.dim_customer_profile
WHERE frequency > 0;

CREATE OR REPLACE VIEW olist_gold.first_order_repurchase AS
SELECT customer_key, first_purchase_date, first_score_int AS score_int, first_score_label, repurchase_90d
FROM olist_gold.dim_customer_profile
WHERE repurchase_observable = 1;

-- Thay bảng tính DAX cust_2nd_gap (gap_bucket / gap_order đang là cột tính trong model).
CREATE OR REPLACE VIEW olist_gold.cust_2nd_gap AS
SELECT customer_key, days_to_2nd, gap_bucket, gap_order
FROM olist_gold.dim_customer_profile
WHERE days_to_2nd IS NOT NULL;

-- dim_seller: giữ đúng cột CSV cũ; n_categories / category_band đang là cột tính DAX trong model,
-- bảng vật lý olist_gold.dim_seller đã có sẵn nếu muốn bỏ cột tính đó.
CREATE OR REPLACE VIEW olist_gold.v_dim_seller AS
SELECT seller_id, seller_state, seller_region, seller_region_order, gmv, orders, gmv_rank, seller_tier,
       seller_tier_order, first_month, first_year, seller_cohort, seller_cohort_order, retention_observable,
       active_m6_8, avg_review, negative_rate, late_rate, review_band, review_band_order
FROM olist_gold.dim_seller;
