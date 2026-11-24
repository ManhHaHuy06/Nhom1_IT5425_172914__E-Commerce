-- Reload ClickHouse olist_gold_seller from Spark Parquet exports (run after init_seller.sql).
-- Loads all 27 __new staging tables first, then atomically EXCHANGEs into serving tables.

TRUNCATE TABLE olist_gold_seller.sales_detail__new;
INSERT INTO olist_gold_seller.sales_detail__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/sales_detail/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.orders__new;
INSERT INTO olist_gold_seller.orders__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/orders/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.payments_by_order__new;
INSERT INTO olist_gold_seller.payments_by_order__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/payments_by_order/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.reviews__new;
INSERT INTO olist_gold_seller.reviews__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/reviews/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.seller_order__new;
INSERT INTO olist_gold_seller.seller_order__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/seller_order/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.customer_order_bridge__new;
INSERT INTO olist_gold_seller.customer_order_bridge__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/customer_order_bridge/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.sales_performance_daily__new;
INSERT INTO olist_gold_seller.sales_performance_daily__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/sales_performance_daily/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.sales_performance_monthly__new;
INSERT INTO olist_gold_seller.sales_performance_monthly__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/sales_performance_monthly/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.category_performance_daily__new;
INSERT INTO olist_gold_seller.category_performance_daily__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/category_performance_daily/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.category_performance_monthly__new;
INSERT INTO olist_gold_seller.category_performance_monthly__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/category_performance_monthly/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.product_performance_monthly__new;
INSERT INTO olist_gold_seller.product_performance_monthly__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/product_performance_monthly/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.geography_performance_daily__new;
INSERT INTO olist_gold_seller.geography_performance_daily__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/geography_performance_daily/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.geography_performance_monthly__new;
INSERT INTO olist_gold_seller.geography_performance_monthly__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/geography_performance_monthly/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.order_status_daily__new;
INSERT INTO olist_gold_seller.order_status_daily__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/order_status_daily/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.order_status_monthly__new;
INSERT INTO olist_gold_seller.order_status_monthly__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/order_status_monthly/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.delivery_performance_daily__new;
INSERT INTO olist_gold_seller.delivery_performance_daily__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/delivery_performance_daily/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.delivery_performance_monthly__new;
INSERT INTO olist_gold_seller.delivery_performance_monthly__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/delivery_performance_monthly/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.order_hourly_daily__new;
INSERT INTO olist_gold_seller.order_hourly_daily__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/order_hourly_daily/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.order_hourly_monthly__new;
INSERT INTO olist_gold_seller.order_hourly_monthly__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/order_hourly_monthly/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.order_weekday_monthly__new;
INSERT INTO olist_gold_seller.order_weekday_monthly__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/order_weekday_monthly/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.review_score_daily__new;
INSERT INTO olist_gold_seller.review_score_daily__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/review_score_daily/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.review_score_monthly__new;
INSERT INTO olist_gold_seller.review_score_monthly__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/review_score_monthly/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.review_geography_monthly__new;
INSERT INTO olist_gold_seller.review_geography_monthly__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/review_geography_monthly/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.review_category_monthly__new;
INSERT INTO olist_gold_seller.review_category_monthly__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/review_category_monthly/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.customer_lifetime__new;
INSERT INTO olist_gold_seller.customer_lifetime__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/customer_lifetime/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.customer_activity_monthly__new;
INSERT INTO olist_gold_seller.customer_activity_monthly__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/customer_activity_monthly/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

TRUNCATE TABLE olist_gold_seller.customer_activity_daily__new;
INSERT INTO olist_gold_seller.customer_activity_daily__new SELECT * FROM s3('http://minio:9000/lakehouse/exports/gold_seller/customer_activity_daily/*.parquet', 'Parquet') SETTINGS insert_null_as_default = 0;

-- Atomic swap after all 27 loads succeed
EXCHANGE TABLES olist_gold_seller.sales_detail__new AND olist_gold_seller.sales_detail;
EXCHANGE TABLES olist_gold_seller.orders__new AND olist_gold_seller.orders;
EXCHANGE TABLES olist_gold_seller.payments_by_order__new AND olist_gold_seller.payments_by_order;
EXCHANGE TABLES olist_gold_seller.reviews__new AND olist_gold_seller.reviews;
EXCHANGE TABLES olist_gold_seller.seller_order__new AND olist_gold_seller.seller_order;
EXCHANGE TABLES olist_gold_seller.customer_order_bridge__new AND olist_gold_seller.customer_order_bridge;
EXCHANGE TABLES olist_gold_seller.sales_performance_daily__new AND olist_gold_seller.sales_performance_daily;
EXCHANGE TABLES olist_gold_seller.sales_performance_monthly__new AND olist_gold_seller.sales_performance_monthly;
EXCHANGE TABLES olist_gold_seller.category_performance_daily__new AND olist_gold_seller.category_performance_daily;
EXCHANGE TABLES olist_gold_seller.category_performance_monthly__new AND olist_gold_seller.category_performance_monthly;
EXCHANGE TABLES olist_gold_seller.product_performance_monthly__new AND olist_gold_seller.product_performance_monthly;
EXCHANGE TABLES olist_gold_seller.geography_performance_daily__new AND olist_gold_seller.geography_performance_daily;
EXCHANGE TABLES olist_gold_seller.geography_performance_monthly__new AND olist_gold_seller.geography_performance_monthly;
EXCHANGE TABLES olist_gold_seller.order_status_daily__new AND olist_gold_seller.order_status_daily;
EXCHANGE TABLES olist_gold_seller.order_status_monthly__new AND olist_gold_seller.order_status_monthly;
EXCHANGE TABLES olist_gold_seller.delivery_performance_daily__new AND olist_gold_seller.delivery_performance_daily;
EXCHANGE TABLES olist_gold_seller.delivery_performance_monthly__new AND olist_gold_seller.delivery_performance_monthly;
EXCHANGE TABLES olist_gold_seller.order_hourly_daily__new AND olist_gold_seller.order_hourly_daily;
EXCHANGE TABLES olist_gold_seller.order_hourly_monthly__new AND olist_gold_seller.order_hourly_monthly;
EXCHANGE TABLES olist_gold_seller.order_weekday_monthly__new AND olist_gold_seller.order_weekday_monthly;
EXCHANGE TABLES olist_gold_seller.review_score_daily__new AND olist_gold_seller.review_score_daily;
EXCHANGE TABLES olist_gold_seller.review_score_monthly__new AND olist_gold_seller.review_score_monthly;
EXCHANGE TABLES olist_gold_seller.review_geography_monthly__new AND olist_gold_seller.review_geography_monthly;
EXCHANGE TABLES olist_gold_seller.review_category_monthly__new AND olist_gold_seller.review_category_monthly;
EXCHANGE TABLES olist_gold_seller.customer_lifetime__new AND olist_gold_seller.customer_lifetime;
EXCHANGE TABLES olist_gold_seller.customer_activity_monthly__new AND olist_gold_seller.customer_activity_monthly;
EXCHANGE TABLES olist_gold_seller.customer_activity_daily__new AND olist_gold_seller.customer_activity_daily;
