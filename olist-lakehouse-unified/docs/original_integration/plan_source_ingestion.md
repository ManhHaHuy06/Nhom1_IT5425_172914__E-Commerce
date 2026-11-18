# Source ingestion contract

## Transactional tables

| MySQL table | Bronze target | Extraction |
|---|---|---|
| customers | olist_customers | Incremental `updated_at` |
| orders | olist_orders | Incremental `updated_at` |
| order_items | olist_order_items | Incremental `updated_at` |
| order_payments | olist_order_payments | Incremental `updated_at` |
| order_reviews | olist_order_reviews | Incremental `updated_at` |

Each table has an independent watermark. The extraction upper bound is captured once per DAG run as `NOW(3) - INTERVAL 10 SECOND`. SQL predicate: `updated_at > wm_from AND updated_at <= wm_to`. Source extraction is chunked and written as Parquet. Business timestamps remain the values stored in MySQL; `updated_at` is used only for extraction and is not copied into Bronze.

## Reference tables

| MySQL table | Bronze target | Extraction |
|---|---|---|
| products | olist_products | Full snapshot daily |
| sellers | olist_sellers | Full snapshot daily |
| geolocation | olist_geolocation | Full snapshot daily |
| product_category_name_translation | same name | Full snapshot daily |

Reference tables have no `updated_at` in the source schema. Each snapshot uses the Airflow logical date as `snapshot_id`. Geolocation is streamed in chunks; no whole-table pandas load is performed.

## Exact Bronze contract

The adapter follows `lakehouse_engine/layers/bronze/create_bronze_tables.sql` exactly. It selects the business columns only, casts them to STRING and adds `loaded_at TIMESTAMP`, `source_file STRING`, `batch_id STRING`. It drops source-only technical columns (`created_at`, `updated_at`, `review_pk`, `geo_pk`). It appends into existing tables and never executes DDL.

## Idempotency

Transaction batch ID is a stable SHA-256-derived identifier from the complete watermark window map. Reference batch ID is `reference_YYYYMMDD`. The Spark adapter checks the target table for the batch ID before appending. This is per-table idempotency; it is not a distributed transaction spanning all nine tables.

## Watermark commit

Watermarks and `last_reference_snapshot_date` are committed only after all required ingestion and downstream processing tasks succeed. A no-data interval can commit its upper watermark after successful extraction because no Bronze or downstream changes are pending. A failed run retains the prior state and can be retried.

## Known CDC boundary

This is polling by `updated_at`, not binlog CDC. If a row is updated several times between polling windows, only its latest state is extracted. Hard deletes are not detected. These limits match the current simulator's insert/update behavior.


## Geolocation snapshot safety

The Lakehouse `dimensions.py` currently groups all rows in `olist_geolocation` and computes `source_row_count`; it does not select the latest snapshot. Therefore blindly appending the same full geolocation snapshot every day would multiply the count. Integration fingerprints each full snapshot using an order-independent multiset hash. The first snapshot is appended. Identical later snapshots are still staged and manifested but skipped by the Bronze adapter. If the fingerprint changes, extraction fails before any reference table is appended; this prevents silently mixing old/new geolocation observations under the unchanged Silver transformation. A future changed-geolocation policy requires an explicitly approved controlled Bronze replacement/rebuild or a change to the Lakehouse transformation, neither of which is performed automatically.
