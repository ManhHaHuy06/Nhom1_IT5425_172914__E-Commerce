# Architecture — Olist Integration + Existing Lakehouse

## Responsibility boundary

Integration owns the simulated OLTP MySQL database, virtual-time simulator, Airflow, its own Spark runtime, source ingestion and orchestration. The existing Lakehouse remains the sole owner of the shared MinIO bucket, Iceberg REST Catalog metadata service, existing Iceberg tables, ClickHouse serving database and dashboard data contract.

The complete Lakehouse transformation source is copied into `lakehouse_engine/` from the supplied Lakehouse ZIP: `common/`, `layers/`, and `olist_pipeline.py`. Silver Dimensions, Silver Facts, Gold Mart logic and ClickHouse SQL are not rewritten. A new Bronze OLTP adapter is added because the old Lakehouse Bronze loader reads the manual CSV handoff and is not the new source ingestion path.

## Host-published endpoint connectivity

Integration containers do not join `olist-lakehouse-network` and do not call Lakehouse Spark containers. They connect to host-published endpoints using `host.docker.internal`:

- MinIO S3 API: host port 9000 by default.
- Iceberg REST Catalog: host port 8181 by default.
- ClickHouse HTTP: host port 8123 by default.

The Integration Spark runtime uses Spark 3.5.3, Iceberg runtime 1.5.0 and local mode. It uses the existing REST Catalog and `s3a://lakehouse/` warehouse. It does not create a new catalog or warehouse.

## Source classification

### Transactional / incremental

`customers`, `orders`, `order_items`, `order_payments`, `order_reviews`. Each table has `updated_at`; each has an independent watermark. Extraction window is `(wm_from, wm_to]`, with `wm_to = MySQL NOW(3) - 10 seconds`.

### Reference / daily snapshot

`products`, `sellers`, `geolocation`, `product_category_name_translation`. These have no `updated_at`; each logical day gets a full snapshot with a stable snapshot ID. The first geolocation snapshot is compared with existing Bronze (exact match skips; empty target appends; mismatch fails safely). Later identical snapshots are staged but not appended. A changed snapshot is rejected because the copied Silver transformation aggregates all Bronze geolocation rows.

## Processing path

1. Extract MySQL in bounded chunks into local Parquet files.
2. Upload Parquet and manifest to the existing MinIO bucket under `staging/bronze/`.
3. Spark adapter projects exact Bronze business columns, casts to STRING, adds `loaded_at`, `source_file`, `batch_id`, and appends into the existing Iceberg Bronze tables through the shared REST Catalog.
4. Run copied `olist_pipeline.py --stage silver-dimensions`.
5. Run copied `olist_pipeline.py --stage silver-facts`.
6. Run copied `olist_pipeline.py --stage gold`.
7. Run copied `layers/clickhouse/load_gold.sql` against the existing ClickHouse service. The loader's internal `http://minio:9000` URL remains valid from the ClickHouse container; only its literal credentials are substituted from Integration environment variables at execution time.
8. Commit transaction watermarks and reference snapshot date only after downstream success.

## Orchestration safety

A single DAG serializes all downstream overwrites. The two source extraction/staging branches may run independently; Bronze writes are serialized through the Spark runner; Dimensions precede Facts; Gold follows Facts; ClickHouse follows Gold. If no new rows and no reference snapshot is due, downstream processing is skipped. The final commit task uses `ALL_DONE` only to inspect all upstream task states; it refuses to commit if any task failed or if ClickHouse did not succeed when new data existed.

## Initialization vs operation

DDL scripts are copied as schema references but are not executed by the automated DAG. MinIO, bucket creation, Iceberg REST Catalog, namespaces/tables, ClickHouse database/tables and dashboard are not initialized by Integration. The Lakehouse must already be initialized and running.
