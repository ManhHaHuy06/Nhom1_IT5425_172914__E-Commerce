-- Reload ClickHouse serving tables from Spark Parquet exports on MinIO.
-- Credentials match infrastructure/.env.example (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD).

TRUNCATE TABLE olist_gold.sales_performance_daily;
INSERT INTO olist_gold.sales_performance_daily
SELECT *
FROM s3(
    'http://minio:9000/lakehouse/exports/gold/sales_performance_daily/*.parquet',
    'admin',
    'admin123',
    'Parquet'
);

TRUNCATE TABLE olist_gold.delivery_delay_analysis;
INSERT INTO olist_gold.delivery_delay_analysis
SELECT *
FROM s3(
    'http://minio:9000/lakehouse/exports/gold/delivery_delay_analysis/*.parquet',
    'admin',
    'admin123',
    'Parquet'
);

TRUNCATE TABLE olist_gold.seller_scorecard;
INSERT INTO olist_gold.seller_scorecard
SELECT *
FROM s3(
    'http://minio:9000/lakehouse/exports/gold/seller_scorecard/*.parquet',
    'admin',
    'admin123',
    'Parquet'
);
