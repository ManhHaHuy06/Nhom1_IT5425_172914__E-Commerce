-- GOLD LAYER DDL - marts phục vụ dashboard Power BI (xem docs/GOLD_DESIGN.md).
-- Thứ tự cột ở đây là "hợp đồng": build_marts.py ép kiểu/thứ tự theo schema bảng, Parquet export giữ nguyên thứ tự,
-- và layers/clickhouse/init_gold.sql phải khai báo cùng thứ tự (INSERT ... SELECT * FROM s3() map theo vị trí).

CREATE NAMESPACE IF NOT EXISTS iceberg.gold;

-- Mart mẫu cũ, không visual nào dùng.
DROP TABLE IF EXISTS iceberg.gold.sales_performance_daily;
DROP TABLE IF EXISTS iceberg.gold.delivery_delay_analysis;
DROP TABLE IF EXISTS iceberg.gold.seller_scorecard;

CREATE TABLE IF NOT EXISTS iceberg.gold.report_period (
    report_year                 INT,
    report_month                INT,
    cy_start                    DATE,
    cy_end                      DATE,
    py_start                    DATE,
    py_end                      DATE,
    trend_start                 DATE,
    label_cy                    STRING,
    label_py                    STRING,
    market_growth               DOUBLE,
    category_share_median       DOUBLE,
    state_penetration_median    DOUBLE,
    state_growth_median         DOUBLE,
    built_at                    TIMESTAMP
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.gold.fact_order_summary (
    order_key                   BIGINT,
    order_id                    STRING,
    customer_key                BIGINT,
    order_status                STRING,
    state                       STRING,
    region                      STRING,
    region_order                INT,
    customer_city_key           STRING,
    purchase_date               DATE,
    purchase_hour               INT,
    n_items                     INT,
    n_products                  INT,
    n_sellers                   INT,
    product_value               DOUBLE,
    freight_value               DOUBLE,
    order_gmv                   DOUBLE,
    main_category               STRING,
    payment_type                STRING,
    installments                INT,
    is_valid                    INT,
    order_value_band            STRING,
    band_order                  INT,
    inst_order                  INT,
    installment_band            STRING,
    is_multi_product            INT,
    is_delivered                INT,
    delivery_days               INT,
    estimated_delivery_days     INT,
    delivery_delay_days         INT,
    review_score                DOUBLE,
    is_negative                 INT,
    is_positive                 INT,
    is_late                     INT,
    score_int                   INT,
    score_label                 STRING,
    delay_bucket                STRING,
    delay_bucket_order          INT,
    delivery_status             STRING,
    delivery_days_capped        INT,
    items_band                  STRING,
    items_band_order            INT,
    sellers_band                STRING,
    sellers_band_order          INT
) USING iceberg
PARTITIONED BY (months(purchase_date))
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.gold.fact_item_sales (
    order_key                   BIGINT,
    order_id                    STRING,
    order_item_id               INT,
    product_key                 BIGINT,
    product_id                  STRING,
    seller_key                  BIGINT,
    seller_id                   STRING,
    customer_key                BIGINT,
    purchase_date               DATE,
    category                    STRING,
    price                       DOUBLE,
    freight_value               DOUBLE,
    item_gmv                    DOUBLE,
    price_band                  STRING,
    price_band_order            INT,
    sku_band                    STRING,
    sku_band_order              INT,
    customer_state              STRING,
    seller_state                STRING,
    seller_region               STRING,
    seller_region_order         INT,
    seller_city_key             STRING,
    is_local_seller             INT,
    is_top12_category           STRING
) USING iceberg
PARTITIONED BY (months(purchase_date))
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.gold.dim_date (
    date                        DATE,
    year                        INT,
    month                       INT,
    year_month                  STRING,
    year_month_order            INT,
    dow                         INT,
    weekday                     STRING
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.gold.dim_state (
    state                       STRING,
    region                      STRING,
    region_order                INT,
    population_2018             BIGINT,
    in_matrix                   STRING,
    quadrant                    STRING,
    quadrant_order              INT
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.gold.dim_category (
    category                    STRING,
    category_group              STRING,
    category_group_order        INT,
    in_matrix                   STRING,
    quadrant                    STRING,
    quadrant_order              INT
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.gold.dim_seller (
    seller_key                  BIGINT,
    seller_id                   STRING,
    seller_state                STRING,
    seller_city_key             STRING,
    seller_region               STRING,
    seller_region_order         INT,
    gmv                         DOUBLE,
    orders                      BIGINT,
    gmv_rank                    INT,
    seller_tier                 STRING,
    seller_tier_order           INT,
    first_month                 DATE,
    first_year                  INT,
    seller_cohort               STRING,
    seller_cohort_order         INT,
    retention_observable        STRING,
    active_m6_8                 INT,
    avg_review                  DOUBLE,
    negative_rate               DOUBLE,
    late_rate                   DOUBLE,
    review_band                 STRING,
    review_band_order           INT,
    n_categories                INT,
    category_band               STRING,
    category_band_order         INT
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.gold.dim_customer_profile (
    customer_key                BIGINT,
    customer_unique_id          STRING,
    state                       STRING,
    region                      STRING,
    recency_days                INT,
    frequency                   INT,
    monetary                    DOUBLE,
    R                           INT,
    F                           INT,
    M                           INT,
    segment                     STRING,
    segment_order               INT,
    days_to_2nd                 INT,
    gap_bucket                  STRING,
    gap_order                   INT,
    first_purchase_date         DATE,
    first_score_int             INT,
    first_score_label           STRING,
    repurchase_90d              INT,
    repurchase_observable       INT
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

-- ============================================================================ DỰ ĐOÁN ML (stage `predict`, xem docs/ML_PREDICTIONS.md)
-- Không nằm trong build_marts: do layers/predict/build_predictions.py ghi (Silver + trọng số trên MinIO). Thứ tự cột là hợp đồng
-- với olist-ml/olist_ml/predict_core.py (ORDER_PRED_COLUMNS) và layers/clickhouse/init_gold.sql.
-- 1 dòng / đơn. late_risk dùng để XẾP HẠNG (không phải xác suất). Cờ theo thứ hạng trong cửa sổ 3 ngày trước, chỉ tính trên đơn bán.
CREATE TABLE IF NOT EXISTS iceberg.gold.order_predictions (
    order_id            STRING,
    order_key           BIGINT,
    purchase_date       DATE,
    late_risk           DOUBLE,
    expected_days       DOUBLE,
    safe_promise_days   DOUBLE,
    is_flagged          INT,
    is_escalated        INT,
    model_version       STRING,
    scored_at           TIMESTAMP
) USING iceberg
PARTITIONED BY (months(purchase_date))
TBLPROPERTIES ('write.format.default' = 'parquet');

-- Dự báo số đơn/ngày cho 28 ngày tới, tính từ ngày cuối cùng ĐẦY ĐỦ dữ liệu (origin_date). level = 'ALL' (giai đoạn 1).
CREATE TABLE IF NOT EXISTS iceberg.gold.forecast_daily_orders (
    level           STRING,
    origin_date     DATE,
    target_date     DATE,
    horizon_day     INT,
    forecast        DOUBLE,
    lo80            DOUBLE,
    hi80            DOUBLE,
    model_version   STRING,
    scored_at       TIMESTAMP
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');
