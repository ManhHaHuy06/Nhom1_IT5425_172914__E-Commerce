-- BRONZE LAYER DDL - Olist Data Lakehouse

-- Bronze la lop dau tien (khong co vung raw rieng): append-only, khong sua doi.
-- Nguon: CSV handoff crawler tren MinIO s3a://handoff/bronze/... ,
-- Spark nap vao iceberg.bronze.* kem metadata: loaded_at, source_file, batch_id.
--
-- Chay bang Spark SQL:
--   spark.sql(open("layers/bronze/create_bronze_tables.sql").read())

CREATE NAMESPACE IF NOT EXISTS iceberg.bronze;

CREATE TABLE IF NOT EXISTS iceberg.bronze.olist_orders (
    order_id              STRING,
    customer_id           STRING,
    order_status          STRING,
    order_purchase_timestamp STRING,
    order_approved_at     STRING,
    order_delivered_carrier_date   STRING,
    order_delivered_customer_date  STRING,
    order_estimated_delivery_date  STRING,
    loaded_at            TIMESTAMP,
    source_file          STRING,
    batch_id             STRING
) USING iceberg
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.olist_order_items (
    order_id              STRING,
    order_item_id         STRING,
    product_id            STRING,
    seller_id             STRING,
    shipping_limit_date   STRING,
    price                 STRING,
    freight_value         STRING,
    loaded_at            TIMESTAMP,
    source_file          STRING,
    batch_id             STRING
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.bronze.olist_order_payments (
    order_id              STRING,
    payment_sequential    STRING,
    payment_type          STRING,
    payment_installments  STRING,
    payment_value         STRING,
    loaded_at            TIMESTAMP,
    source_file          STRING,
    batch_id             STRING
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.bronze.olist_order_reviews (
    review_id                    STRING,
    order_id                     STRING,
    review_score                 STRING,
    review_comment_title         STRING,
    review_comment_message       STRING,
    review_creation_date         STRING,
    review_answer_timestamp      STRING,
    loaded_at                   TIMESTAMP,
    source_file                 STRING,
    batch_id                    STRING
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

-- Chu y: customer_id (theo tung don hang) va customer_unique_id (nguoi thuc)
CREATE TABLE IF NOT EXISTS iceberg.bronze.olist_customers (
    customer_id           STRING,
    customer_unique_id    STRING,
    customer_zip_code_prefix STRING,
    customer_city         STRING,
    customer_state        STRING,
    loaded_at            TIMESTAMP,
    source_file          STRING,
    batch_id             STRING
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.bronze.olist_products (
    product_id                     STRING,
    product_category_name          STRING,
    product_name_lenght            STRING,
    product_description_lenght     STRING,
    product_photos_qty             STRING,
    product_weight_g               STRING,
    product_length_cm              STRING,
    product_height_cm              STRING,
    product_width_cm               STRING,
    loaded_at                     TIMESTAMP,
    source_file                   STRING,
    batch_id                      STRING
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.bronze.olist_sellers (
    seller_id                 STRING,
    seller_zip_code_prefix    STRING,
    seller_city               STRING,
    seller_state              STRING,
    loaded_at                TIMESTAMP,
    source_file              STRING,
    batch_id                 STRING
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

-- Chu y: Co >1 trieu dong vi trung lap, xu ly dedup tai Silver
CREATE TABLE IF NOT EXISTS iceberg.bronze.olist_geolocation (
    geolocation_zip_code_prefix   STRING,
    geolocation_lat               STRING,
    geolocation_lng               STRING,
    geolocation_city              STRING,
    geolocation_state             STRING,
    loaded_at                    TIMESTAMP,
    source_file                  STRING,
    batch_id                     STRING
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.bronze.product_category_name_translation (
    product_category_name          STRING,
    product_category_name_english  STRING,
    loaded_at                     TIMESTAMP,
    source_file                   STRING,
    batch_id                      STRING
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');
