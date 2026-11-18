-- LEGACY ONLY - DO NOT EXECUTE.
-- This file contains direct TRUNCATE/INSERT statements and is retained only for audit/reference.
-- Automated refresh is implemented by airflow/include/clickhouse_loader.py using staging tables
-- and EXCHANGE TABLES. The old literal credentials have been replaced with placeholders.
-- Run after build_gold_marts.py has successfully written/exported all Iceberg Gold marts.
-- Path convention: s3a://lakehouse/exports/gold/<table_name> in Spark,
--                 http://minio:9000/lakehouse/exports/gold/<table_name>/*.parquet in ClickHouse.
-- Credentials below follow the legacy load_gold.sql example. Prefer environment-managed
-- secrets in production; do not commit real credentials to source control.

TRUNCATE TABLE olist_gold.sales_detail;
INSERT INTO olist_gold.sales_detail
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/sales_detail/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.orders;
INSERT INTO olist_gold.orders
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/orders/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.payments_by_order;
INSERT INTO olist_gold.payments_by_order
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/payments_by_order/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.reviews;
INSERT INTO olist_gold.reviews
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/reviews/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.seller_order;
INSERT INTO olist_gold.seller_order
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/seller_order/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.customer_order_bridge;
INSERT INTO olist_gold.customer_order_bridge
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/customer_order_bridge/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.sales_performance_daily;
INSERT INTO olist_gold.sales_performance_daily
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/sales_performance_daily/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.sales_performance_monthly;
INSERT INTO olist_gold.sales_performance_monthly
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/sales_performance_monthly/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.category_performance_daily;
INSERT INTO olist_gold.category_performance_daily
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/category_performance_daily/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.category_performance_monthly;
INSERT INTO olist_gold.category_performance_monthly
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/category_performance_monthly/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.product_performance_monthly;
INSERT INTO olist_gold.product_performance_monthly
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/product_performance_monthly/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.geography_performance_daily;
INSERT INTO olist_gold.geography_performance_daily
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/geography_performance_daily/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.geography_performance_monthly;
INSERT INTO olist_gold.geography_performance_monthly
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/geography_performance_monthly/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.order_status_daily;
INSERT INTO olist_gold.order_status_daily
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/order_status_daily/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.order_status_monthly;
INSERT INTO olist_gold.order_status_monthly
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/order_status_monthly/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.delivery_performance_daily;
INSERT INTO olist_gold.delivery_performance_daily
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/delivery_performance_daily/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.delivery_performance_monthly;
INSERT INTO olist_gold.delivery_performance_monthly
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/delivery_performance_monthly/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.order_hourly_daily;
INSERT INTO olist_gold.order_hourly_daily
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/order_hourly_daily/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.order_hourly_monthly;
INSERT INTO olist_gold.order_hourly_monthly
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/order_hourly_monthly/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.order_weekday_monthly;
INSERT INTO olist_gold.order_weekday_monthly
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/order_weekday_monthly/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.review_score_daily;
INSERT INTO olist_gold.review_score_daily
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/review_score_daily/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.review_score_monthly;
INSERT INTO olist_gold.review_score_monthly
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/review_score_monthly/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.review_geography_monthly;
INSERT INTO olist_gold.review_geography_monthly
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/review_geography_monthly/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.review_category_monthly;
INSERT INTO olist_gold.review_category_monthly
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/review_category_monthly/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.customer_lifetime;
INSERT INTO olist_gold.customer_lifetime
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/customer_lifetime/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.customer_activity_monthly;
INSERT INTO olist_gold.customer_activity_monthly
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/customer_activity_monthly/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');

TRUNCATE TABLE olist_gold.customer_activity_daily;
INSERT INTO olist_gold.customer_activity_daily
SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold/customer_activity_daily/*.parquet', 'MINIO_ACCESS_KEY_PLACEHOLDER', 'MINIO_SECRET_KEY_PLACEHOLDER', 'Parquet');
