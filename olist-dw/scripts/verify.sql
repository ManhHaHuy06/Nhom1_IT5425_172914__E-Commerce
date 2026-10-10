USE olist_dw;

SHOW TABLES;

SELECT 'dim_geolocation' AS table_name, COUNT(*) AS rows FROM dim_geolocation
UNION ALL SELECT 'dim_category', COUNT(*) FROM dim_category
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_seller', COUNT(*) FROM dim_seller
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL SELECT 'fact_orders', COUNT(*) FROM fact_orders
UNION ALL SELECT 'fact_order_items', COUNT(*) FROM fact_order_items
UNION ALL SELECT 'fact_payments', COUNT(*) FROM fact_payments
UNION ALL SELECT 'fact_reviews', COUNT(*) FROM fact_reviews
UNION ALL SELECT 'fact_customer_order', COUNT(*) FROM fact_customer_order;

SELECT COUNT(*) AS orphan_orders
FROM fact_orders o
LEFT JOIN dim_customer c ON o.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

SELECT COUNT(*) AS orphan_order_items
FROM fact_order_items i
LEFT JOIN fact_orders o ON i.order_key = o.order_key
WHERE o.order_key IS NULL;

SELECT COUNT(*) AS orphan_products
FROM fact_order_items i
LEFT JOIN dim_product p ON i.product_key = p.product_key
WHERE p.product_key IS NULL;

SELECT COUNT(*) AS orphan_sellers
FROM fact_order_items i
LEFT JOIN dim_seller s ON i.seller_key = s.seller_key
WHERE s.seller_key IS NULL;
