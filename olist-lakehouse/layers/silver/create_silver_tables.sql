-- SILVER LAYER DDL (3NF Entity Model) + QUARANTINE DDL
-- Pure entity model using natural keys (no Kimball dim_/fact_, no xxhash64 surrogate keys, no aggregations).
-- Rejected rows from Silver DQ checks (null PK, timeline A1-A6, orphan FKs) go to iceberg.quarantine.silver_rejected_records.

CREATE NAMESPACE IF NOT EXISTS iceberg.silver;
CREATE NAMESPACE IF NOT EXISTS iceberg.quarantine;

-- Remove legacy Kimball tables from previous Silver schema.
DROP TABLE IF EXISTS iceberg.silver.dim_geolocation;
DROP TABLE IF EXISTS iceberg.silver.dim_category;
DROP TABLE IF EXISTS iceberg.silver.dim_customer;
DROP TABLE IF EXISTS iceberg.silver.dim_seller;
DROP TABLE IF EXISTS iceberg.silver.dim_product;
DROP TABLE IF EXISTS iceberg.silver.dim_date;
DROP TABLE IF EXISTS iceberg.silver.fact_orders;
DROP TABLE IF EXISTS iceberg.silver.fact_order_items;
DROP TABLE IF EXISTS iceberg.silver.fact_payments;
DROP TABLE IF EXISTS iceberg.silver.fact_reviews;
DROP TABLE IF EXISTS iceberg.silver.fact_customer_order;

-- Master entities
CREATE TABLE IF NOT EXISTS iceberg.silver.geolocation (
    zip_code_prefix     INT,
    city                STRING,
    state               STRING,
    latitude            DOUBLE,
    longitude           DOUBLE,
    source_row_count    BIGINT
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.categories (
    product_category_name           STRING,
    product_category_name_english   STRING
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.customers (
    customer_unique_id  STRING,
    zip_code_prefix     INT,
    city                STRING,
    state               STRING
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.sellers (
    seller_id           STRING,
    zip_code_prefix     INT,
    city                STRING,
    state               STRING
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.products (
    product_id                   STRING,
    product_category_name        STRING,
    product_name_length          INT,
    product_description_length   INT,
    product_photos_qty           INT,
    product_weight_g             DOUBLE,
    product_length_cm            DOUBLE,
    product_height_cm            DOUBLE,
    product_width_cm             DOUBLE
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

-- Transactional entities
CREATE TABLE IF NOT EXISTS iceberg.silver.orders (
    order_id                          STRING,
    customer_id                       STRING,
    customer_unique_id                STRING,
    order_status                      STRING,
    order_purchase_timestamp          TIMESTAMP,
    order_approved_at                 TIMESTAMP,
    order_delivered_carrier_date      TIMESTAMP,
    order_delivered_customer_date     TIMESTAMP,
    order_estimated_delivery_date     TIMESTAMP,
    delivery_days                     INT,
    estimated_delivery_days           INT,
    delivery_delay_days               INT,
    is_delivered                      INT,
    is_canceled                       INT
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.order_items (
    order_id                STRING,
    order_item_id           INT,
    product_id              STRING,
    seller_id               STRING,
    shipping_limit_date     TIMESTAMP,
    price                   DOUBLE,
    freight_value           DOUBLE,
    item_total              DOUBLE
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.order_payments (
    order_id                STRING,
    payment_sequential      INT,
    payment_type            STRING,
    payment_installments    INT,
    payment_value           DOUBLE
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.order_reviews (
    review_id                   STRING,
    order_id                    STRING,
    review_score                INT,
    review_comment_title        STRING,
    review_comment_message      STRING,
    review_creation_date        TIMESTAMP,
    review_answer_timestamp     TIMESTAMP,
    is_positive                 INT,
    is_negative                 INT
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

-- Data quality quarantine
CREATE TABLE IF NOT EXISTS iceberg.quarantine.silver_rejected_records (
    quarantined_at      TIMESTAMP,
    batch_id            STRING,
    source_table        STRING,
    target_table        STRING,
    record_key          STRING,
    rule_code           STRING,
    reason              STRING,
    source_updated_at   STRING,
    source_file         STRING,
    raw_payload         STRING
) USING iceberg
PARTITIONED BY (target_table)
TBLPROPERTIES ('write.format.default' = 'parquet');
