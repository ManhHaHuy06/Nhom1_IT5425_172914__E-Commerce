# Olist ETL specification

## Grain

| Table | Grain |
|---|---|
| dim_customer | one row per customer_unique_id |
| dim_product | one row per product_id |
| dim_seller | one row per seller_id |
| dim_category | one row per product_category_name |
| dim_geolocation | one row per ZIP prefix |
| dim_date | one row per date |
| fact_orders | one row per order_id |
| fact_order_items | one row per order_id + order_item_id |
| fact_payments | one row per order_id + payment_sequential |
| fact_reviews | one row per review_id |
| fact_customer_order | one row per customer |

## Join rules

- orders N:1 customers through customer_id -> customer_unique_id -> customer_key
- order_items N:1 products through product_id
- order_items N:1 sellers through seller_id
- products N:1 categories after category key is unique
- customers N:1 geolocation only after geolocation is aggregated to one row per ZIP prefix
- sellers N:1 geolocation only after geolocation is aggregated to one row per ZIP prefix
- orders 1:N order_items: do not join into fact_orders unless the N side is aggregated first
- orders 1:N payments: keep fact_payments separate; aggregate first for order-level payment metrics
- orders 1:N reviews: keep fact_reviews separate; aggregate first for order-level review metrics

## Why this prevents fan-out

Joining orders, items, payments and reviews directly can create:

    item_count x payment_count x review_count

rows for one order. This changes the grain and inflates metrics.

## Cleaning

- explicit Spark schemas
- trim empty strings to null
- required key validation
- domain validation for order status, coordinates, scores and non-negative monetary values
- business-key deduplication
- invalid records to quarantine

## Transformations

- date_key, year, quarter, month, week and weekend attributes
- delivery_days
- estimated_delivery_days
- delivery_delay_days
- is_delivered / is_canceled
- item_total = price + freight_value
- positive/negative review flags
- customer-level order/spending aggregates

## Output

All curated outputs are Parquet with Snappy compression.
RAW is never overwritten by this project.
