-- Reload ClickHouse serving tables from Spark Parquet exports on MinIO (chạy sau init_gold.sql).
-- MinIO credentials: không ghi ở đây. infrastructure/clickhouse/config.xml (<s3><minio>) gắn
-- MINIO_ROOT_USER / MINIO_ROOT_PASSWORD của container cho mọi URL bắt đầu bằng http://minio:9000/.
-- insert_null_as_default = 0: NULL vào cột không Nullable sẽ báo lỗi thay vì âm thầm thành 0 / ''.
-- Nạp vào <t>__new, chỉ EXCHANGE sang bảng phục vụ khi cả 10 bảng nạp xong. --multiquery dừng ở lỗi đầu tiên,
-- nên lỗi giữa chừng để bảng phục vụ giữ dữ liệu cũ thay vì rỗng.

TRUNCATE TABLE olist_gold.report_period__new;
INSERT INTO olist_gold.report_period__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/report_period/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold.fact_order_summary__new;
INSERT INTO olist_gold.fact_order_summary__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/fact_order_summary/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold.fact_item_sales__new;
INSERT INTO olist_gold.fact_item_sales__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/fact_item_sales/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold.dim_date__new;
INSERT INTO olist_gold.dim_date__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/dim_date/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold.dim_state__new;
INSERT INTO olist_gold.dim_state__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/dim_state/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold.dim_category__new;
INSERT INTO olist_gold.dim_category__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/dim_category/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold.dim_seller__new;
INSERT INTO olist_gold.dim_seller__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/dim_seller/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold.dim_customer_profile__new;
INSERT INTO olist_gold.dim_customer_profile__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/dim_customer_profile/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold.order_predictions__new;
INSERT INTO olist_gold.order_predictions__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/order_predictions/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold.forecast_daily_orders__new;
INSERT INTO olist_gold.forecast_daily_orders__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/forecast_daily_orders/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

EXCHANGE TABLES olist_gold.report_period__new AND olist_gold.report_period;
EXCHANGE TABLES olist_gold.fact_order_summary__new AND olist_gold.fact_order_summary;
EXCHANGE TABLES olist_gold.fact_item_sales__new AND olist_gold.fact_item_sales;
EXCHANGE TABLES olist_gold.dim_date__new AND olist_gold.dim_date;
EXCHANGE TABLES olist_gold.dim_state__new AND olist_gold.dim_state;
EXCHANGE TABLES olist_gold.dim_category__new AND olist_gold.dim_category;
EXCHANGE TABLES olist_gold.dim_seller__new AND olist_gold.dim_seller;
EXCHANGE TABLES olist_gold.dim_customer_profile__new AND olist_gold.dim_customer_profile;
EXCHANGE TABLES olist_gold.order_predictions__new AND olist_gold.order_predictions;
EXCHANGE TABLES olist_gold.forecast_daily_orders__new AND olist_gold.forecast_daily_orders;
