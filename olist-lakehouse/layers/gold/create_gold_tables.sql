-- GOLD LAYER DDL - simple business marts from Silver (examples).
-- Keep column names/order aligned with layers/clickhouse/init_gold.sql for Parquet load.

CREATE NAMESPACE IF NOT EXISTS iceberg.gold;

CREATE TABLE IF NOT EXISTS iceberg.gold.sales_performance_daily (
    sale_date                         DATE,
    product_category_name_english     STRING,
    customer_state                    STRING,
    order_count                       BIGINT,
    item_count                        BIGINT,
    total_revenue                     DOUBLE,
    total_freight                     DOUBLE,
    avg_freight                       DOUBLE
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.gold.delivery_delay_analysis (
    customer_state                    STRING,
    seller_id                         STRING,
    delivered_orders                  BIGINT,
    late_orders                       BIGINT,
    late_rate                         DOUBLE,
    avg_delay_days                    DOUBLE,
    avg_delivery_days                 DOUBLE
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.gold.seller_scorecard (
    seller_id                         STRING,
    seller_state                      STRING,
    order_count                       BIGINT,
    item_count                        BIGINT,
    total_revenue                     DOUBLE,
    canceled_orders                   BIGINT,
    cancel_rate                       DOUBLE,
    avg_review_score                  DOUBLE,
    avg_carrier_handoff_days          DOUBLE
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');
