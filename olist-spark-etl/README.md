# Olist Spark ETL

Pipeline:

MinIO RAW -> Spark Validation/Cleaning -> Quarantine + CURATED -> Apache Doris

This project reads the Olist data already crawled into MinIO bucket `olist`.
It does not modify RAW. It writes Parquet to `curated/` and bad records to `quarantine/`.

## Expected source layout

- raw/orders/date=YYYY-MM-DD/orders.csv
- raw/order_items/date=YYYY-MM-DD/order_items.csv
- raw/payments/date=YYYY-MM-DD/payments.csv
- raw/reviews/date=YYYY-MM-DD/reviews.csv
- raw/products/version=N/pages/part-*.csv
- raw/sellers/version=N/pages/part-*.csv
- raw/customers/version=N/pages/part-*.csv
- raw/geolocation/version=N/pages/part-*.csv
- raw/categories/version=N/pages/part-*.csv

Master datasets are read recursively, so the exact version directory does not have to be hard-coded.

## Processing

1. Explicit schemas and type conversion
2. Required-field validation
3. Domain validation
4. Duplicate/business-key handling
5. Referential integrity checks
6. Geolocation reduction to one row per ZIP prefix before joins
7. Dimension construction
8. Fact construction
9. Derived attributes and aggregates
10. Row-count / uniqueness quality checks
11. Parquet + Snappy output to CURATED

## Output

curated/dim_customer
curated/dim_product
curated/dim_seller
curated/dim_geolocation
curated/dim_category
curated/dim_date
curated/fact_orders
curated/fact_order_items
curated/fact_payments
curated/fact_reviews
curated/fact_customer_order

## Run

From this directory:

    docker compose up -d --build
    docker compose run --rm spark-etl

Logs:

    docker compose logs -f spark-etl

MinIO console:

    http://localhost:9001

Credentials are the same as the crawler by default: admin / minioadmin.
