# Implementation log — Integration pipeline

## Current package design

- Preserved MySQL simulator source, schema and CLI from the supplied Integration ZIP.
- Kept `Data/data_description.md`; no source CSV files are included.
- Removed the old ingestion DAGs that wrote to the obsolete `olist-lake` bucket and did not load Iceberg.
- Added one serialized end-to-end DAG with separate tasks for transactional extraction, reference snapshot, staging, Bronze, Silver Dimensions, Silver Facts, Gold, ClickHouse and state commit.
- Copied `common/`, `layers/`, and `olist_pipeline.py` from the supplied Lakehouse ZIP into `lakehouse_engine/`. The transformation source files and DDL are retained as source copies. No DDL is executed by the new DAG.
- Added a new OLTP-to-Bronze adapter using the existing Bronze DDL contract.
- Added an Integration-owned Spark 3.5.3 / Iceberg 1.5.0 local runtime. It uses the existing Lakehouse REST Catalog and MinIO via host-published endpoints; it does not join the Lakehouse Docker network or use its Spark Master/Worker.
- Added ClickHouse HTTP loader client that executes the copied 27-table `load_gold.sql` against the existing ClickHouse service. At runtime it substitutes MinIO credentials from `.env`; the copied SQL file itself remains unchanged.
- Removed the standalone MinIO service and its initialization files from Integration.

## Validation status

Static checks are run when producing this archive: Python syntax/AST, Compose YAML parse, copied-source hash comparison, Bronze mapping/DDL column comparison, Gold mart count and ClickHouse loader statement count, and dataset absence.

The authoring environment cannot run the user's Docker Desktop services. Therefore a successful Docker build, host endpoint connectivity, Iceberg REST catalog operations, Spark ETL, ClickHouse reload and dashboard refresh must be verified on the user's machine after configuring `.env`. The ZIP must not be described as runtime-tested until those checks are completed.
