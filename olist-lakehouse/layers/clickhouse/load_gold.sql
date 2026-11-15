-- Reload ClickHouse serving tables from Spark Parquet exports on MinIO (chạy sau init_gold.sql).
-- MinIO credentials: không ghi ở đây. infrastructure/clickhouse/config.xml (<s3><minio>) gắn
-- MINIO_ROOT_USER / MINIO_ROOT_PASSWORD của container cho mọi URL bắt đầu bằng http://minio:9000/.
-- insert_null_as_default = 0: NULL vào cột không Nullable sẽ báo lỗi thay vì âm thầm thành 0 / ''.

TRUNCATE TABLE olist_gold.report_period;
INSERT INTO olist_gold.report_period SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/report_period/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold.fact_order_summary;
INSERT INTO olist_gold.fact_order_summary SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/fact_order_summary/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold.fact_item_sales;
INSERT INTO olist_gold.fact_item_sales SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/fact_item_sales/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold.dim_date;
INSERT INTO olist_gold.dim_date SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/dim_date/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold.dim_state;
INSERT INTO olist_gold.dim_state SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/dim_state/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold.dim_category;
INSERT INTO olist_gold.dim_category SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/dim_category/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold.dim_seller;
INSERT INTO olist_gold.dim_seller SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/dim_seller/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold.dim_customer_profile;
INSERT INTO olist_gold.dim_customer_profile SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/dim_customer_profile/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;
