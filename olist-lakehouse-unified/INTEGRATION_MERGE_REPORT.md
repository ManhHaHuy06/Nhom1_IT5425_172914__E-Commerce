# Integration Merge Report

## Delivered architecture

- One root `docker-compose.yml` for Lakehouse infrastructure, OLTP MySQL, simulator, Airflow, Spark Job Runner and Streamlit dashboard.
- Existing MinIO, Iceberg REST, Spark Master/Worker, Trino and ClickHouse definitions are reused; all services join `olist-lakehouse-network`.
- OLTP database volume reuses `olist-integration_oltp_data` so an existing Integration MySQL database can be retained.
- Airflow metadata/log/work volumes are new to avoid inheriting potentially unsafe old watermarks.
- MinIO initialization is non-destructive and only ensures `handoff` and `lakehouse` buckets exist; it no longer removes legacy buckets.

## Existing Lakehouse logic preserved byte-for-byte

The following files match the supplied `olist-lakehouse(1).zip` exactly:
- `olist_pipeline.py`
- `layers/bronze/create_bronze_tables.sql`
- `layers/silver/create_silver_tables.sql`
- `layers/silver/dimensions.py`
- `layers/silver/facts.py`
- `layers/gold/create_gold_tables.sql`
- `layers/gold/build_marts.py`

No Silver or Gold business transformations were rewritten.

## Integrated components

- MySQL OLTP 3NF schema and least-privilege simulator/ETL users.
- Synthetic seed generator because the supplied archives did not include the Olist CSV source files.
- Virtual-time simulator for new orders, order status transitions, payments and reviews.
- Incremental extraction of five transactional tables using `updated_at` and a 10-second safety lag.
- Daily full snapshots of four reference tables.
- Chunked Parquet staging in MinIO with manifest and retry-safe prefix replacement.
- Spark adapter mapping nine OLTP tables to existing Bronze DDL with STRING business fields and the three required metadata columns.
- Per-table Bronze batch idempotency.
- Dimensions -> Facts -> Gold serialized through the existing Lakehouse Spark pipeline.
- Watermark/snapshot state validation and a single Airflow metadata DB transaction for Variable updates.
- Run-scoped cleanup that only executes after a successful commit.
- ClickHouse staged refresh with Atomic table exchanges; no direct TRUNCATE of live tables. The former destructive SQL is archived as `load_gold_legacy_truncate.sql` with placeholder credentials, and the active `load_gold.sql` is a non-executable handoff note.
- Dashboard container with periodic refresh.

## Validation performed

- 12/12 local unit tests passed.
- Python `compileall` passed for `airflow`, `source`, `layers` and `spark_runner`.
- Compose YAML parsed successfully.
- All Compose bind-mount paths exist.
- Every Compose `${VARIABLE}` is declared in `.env.example`.
- OLTP adapter mappings were compared against the existing Bronze DDL.
- All 27 Gold table names were compared between `build_marts.py` and the ClickHouse loader.

## Not runtime-validated

This build environment does not contain Docker Engine/Compose, so images were not built and containers/DAGs were not run. The ZIP is code- and configuration-validated, not a claim of successful end-to-end execution against the user's persistent volumes.

## Important operational constraints

1. Set root `.env` credentials to match existing Lakehouse persistent volumes.
2. Import old Integration `OLTP_*` credentials before reusing its MySQL volume; `setup.ps1` attempts the sibling path automatically and accepts `-IntegrationEnvPath`.
3. The source CSVs were absent. Fresh OLTP bootstrap therefore creates synthetic Olist-like data, not the original public dataset.
4. Existing populated Bronze geolocation is preserved when the source snapshot differs because the unchanged Silver logic aggregates all Bronze geolocation rows. A geolocation change requires an approved rebuild.
5. ClickHouse exchanges are atomic per table, not as one 27-table transaction. If an exchange fails mid-sequence, stage tables are retained for manual recovery.
6. No Docker build, DAG execution, MySQL write, MinIO write, Iceberg write or ClickHouse write was performed while preparing this ZIP.
