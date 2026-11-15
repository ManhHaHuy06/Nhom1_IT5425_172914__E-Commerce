-- SILVER LAYER DDL - mirrors Nhom1_IT5425_172914__E-Commerce/schema.sql (olist_dw)
-- Iceberg equivalents: VARCHAR/TEXT -> STRING, DATETIME -> TIMESTAMP, TINYINT -> INT.
-- No FK / PK enforcement in Iceberg (documented in schema.sql only).
-- Surrogate keys: xxhash64() in Spark.

CREATE NAMESPACE IF NOT EXISTS iceberg.silver;

CREATE TABLE IF NOT EXISTS iceberg.silver.dim_geolocation (
    geolocation_key     BIGINT,
    zip_code_prefix     INT,
    city                STRING,
    state               STRING,
    latitude            DOUBLE,
    longitude           DOUBLE,
    source_row_count    BIGINT
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.dim_category (
    category_key                    BIGINT,
    product_category_name           STRING,
    product_category_name_english   STRING
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.dim_customer (
    customer_key        BIGINT,
    customer_unique_id  STRING,
    city                STRING,
    state               STRING,
    geolocation_key     BIGINT
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.dim_seller (
    seller_key          BIGINT,
    seller_id           STRING,
    city                STRING,
    state               STRING,
    geolocation_key     BIGINT
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.dim_product (
    product_key                  BIGINT,
    product_id                   STRING,
    category_key                 BIGINT,
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

CREATE TABLE IF NOT EXISTS iceberg.silver.dim_date (
    date_key        INT,
    date            DATE,
    year            INT,
    quarter         INT,
    month           INT,
    month_name      STRING,
    week_of_year    INT,
    day             INT,
    day_of_week     INT,
    is_weekend      BOOLEAN
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.fact_orders (
    order_key                         BIGINT,
    order_id                          STRING,
    customer_key                      BIGINT,
    purchase_date_key                 INT,
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

CREATE TABLE IF NOT EXISTS iceberg.silver.fact_order_items (
    item_key                BIGINT,
    order_key               BIGINT,
    order_id                STRING,
    order_item_id           INT,
    product_key             BIGINT,
    seller_key              BIGINT,
    date_key                INT,
    shipping_limit_date     TIMESTAMP,
    price                   DOUBLE,
    freight_value           DOUBLE,
    item_total              DOUBLE
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.fact_payments (
    payment_key             BIGINT,
    order_key               BIGINT,
    order_id                STRING,
    payment_sequential      INT,
    payment_type            STRING,
    payment_installments    INT,
    payment_value           DOUBLE
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.fact_reviews (
    review_key                  BIGINT,
    order_key                   BIGINT,
    order_id                    STRING,
    review_id                   STRING,
    review_score                INT,
    review_comment_title        STRING,
    review_comment_message      STRING,
    review_creation_date        TIMESTAMP,
    review_answer_timestamp     TIMESTAMP,
    is_positive                 INT,
    is_negative                 INT
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS iceberg.silver.fact_customer_order (
    customer_order_key          BIGINT,
    customer_key                BIGINT,
    order_count                 BIGINT,
    delivered_order_count       BIGINT,
    canceled_order_count        BIGINT,
    total_spent                 DOUBLE,
    average_order_value         DOUBLE,
    first_order_timestamp       TIMESTAMP,
    last_order_timestamp        TIMESTAMP,
    average_delivery_days       DOUBLE
) USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet');
