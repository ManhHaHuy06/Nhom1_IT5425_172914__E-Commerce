# AGENTS.md — Olist E-Commerce Data Platform

Canonical reference for coding agents. Repository root contains three independent sub-projects joined only by
object-storage paths, table names and column order. There is no root build and no test suite.
Human docs are mostly Vietnamese; code comments mix Vietnamese/English.

---

## 1. System Overview & Tech Stack

Batch analytics platform over the public Olist (Brazil) e-commerce dataset. `hosting_server` simulates a live shop:
a replayer writes the Olist CSV history into an OLTP MySQL DB in virtual time; Airflow incrementally extracts
changed rows (watermark on `updated_at`) to Parquet in a MinIO Landing Zone (`s3://olist-lake/landing/`), verifies
completeness hourly, and schedules the cross-layer lakehouse ETL (`etl_lakehouse_medallion`). `olist-lakehouse` reads
that Landing Zone with Spark into an Iceberg medallion (Bronze append-only → Silver 3NF + Quarantine → Gold Kimball
Market + Seller) on a second MinIO, exports Gold to Parquet and bulk-loads it atomically into ClickHouse (`olist_gold`
and `olist_gold_seller`). `serving/` holds both consumption branches: `serving/olist-eda` (5 Jupyter EDA notebooks
querying Silver 3NF via Trino + Power BI PBIP importing `olist_gold` from ClickHouse HTTP) and
`serving/seller-dashboard` (Streamlit + Plotly app querying `olist_gold_seller` via `clickhouse-connect`). Style: two
Docker Compose stacks, batch ELT, Medallion Lakehouse (Bronze append-only, Silver/Gold full rebuild).

| Area | Tech (exact versions where pinned) |
|---|---|
| OLTP source | MySQL 8.0 (`mysql:8.0`), UTC, binlog ROW enabled (CDC not implemented) |
| Simulator | Python 3.11-slim, pandas 2.2.3, pyarrow 18.1.0, PyMySQL 1.1.1, cryptography 43.0.3 |
| Orchestration | Apache Airflow 2.10.5 (python3.11), LocalExecutor, Postgres 15 metadata DB, TaskFlow API + dynamic task mapping |
| Source lake | MinIO built from source (`hosting_server/infra/minio/Dockerfile`, golang:1.24 → alpine:3.20), boto3 S3 client |
| Lakehouse storage | MinIO `soldevelo/minio:2026.7.17` (official images unavailable) |
| Table format / catalog | Apache Iceberg 1.5.0, `apache/iceberg-rest-fixture` REST catalog backed by JDBC on `postgres:15-alpine` |
| Processing | Apache Spark 3.5.3 standalone (1 master + 1 worker), PySpark, hadoop-aws 3.3.4, AWS SDK v1 1.12.262 + v2 2.20.131 |
| Query engine | Trino 440 (Iceberg REST connector, native S3) |
| Serving | ClickHouse 24.3 (MergeTree tables + views), `s3()` table function for loads |
| BI | Power BI Desktop PBIP (TMDL semantic model + PBIR report), M `Web.Contents` → ClickHouse HTTP `:8123` |
| EDA | Jupyter, `trino` Python client, pandas, matplotlib, seaborn |
| Runner | Windows batch `olist-lakehouse/run_lakehouse.bat` (no Makefile / CI) |

---

## 2. Repository Map

```text
refactor_project/
├── hosting_server/                         # Stack 1: OLTP simulator + Airflow ingestion → MinIO Bronze (Parquet)
│   ├── docker-compose.yml                  # project name "olist": oltp-mysql, minio, minio-init, simulator(profile sim), airflow-*
│   ├── .env.example                        # template; MYSQL_* (Aiven) block is stale/unused by compose
│   ├── Data/                               # 9 raw Olist CSVs (read-only input; mounted at /data in simulator)
│   │   └── data_description.md             # dataset dictionary + row counts
│   ├── source/                             # simulator image (olist-source:local), mounted at /app
│   │   ├── sql/01_schema.sql               # OLTP DDL (auto-run by MySQL entrypoint on empty volume only)
│   │   ├── sql/02_users.sh                 # creates sim_rw (RW) and etl_ro (SELECT) users
│   │   ├── olist_data.py                   # CSV map, status lifecycle, EVENT_KINDS/PRIORITY, MILESTONES constants
│   │   ├── db.py                           # pymysql connect() as OLTP_SIM_USER; py()/to_rows()/insert_ignore()
│   │   ├── seed.py                         # seeds 4 reference tables (INSERT IGNORE; geolocation only if empty)
│   │   ├── build_events.py                 # CSV → events.parquet (time-ordered replay events) + self-asserts
│   │   ├── events.parquet                  # generated artifact (~15 MB, gitignored)
│   │   ├── replay.py                       # Replayer: applies event slices to OLTP (idempotent SQL)
│   │   ├── clock.py                        # VirtualClock: v0 + speed*(wall - wall0)
│   │   ├── run.py                          # CLI: bootstrap | live | pause | resume | status
│   │   └── verify_source.py                # reconciliation OLTP vs CSV (counts, status, timestamps, sums)
│   ├── airflow/
│   │   ├── Dockerfile                      # adds pymysql cryptography pyarrow boto3 sqlalchemy pandas docker (constraints-2.10.5)
│   │   ├── dags/ingest_oltp_to_landing.py  # */15 min incremental extract -> landing/, watermark Variables wm__<table>
│   │   ├── dags/ingest_reference_snapshot.py # @daily full snapshot of reference tables -> landing/ref_*
│   │   ├── dags/reconcile_landing.py       # @hourly check: OLTP versions <= wm__<table> all present in Landing
│   │   ├── dags/etl_lakehouse_medallion.py # `5 * * * *` (hourly at :05 UTC, + ShortCircuit) cross-layer ETL: ddl -> bronze -> silver -> gold -> clickhouse
│   │   └── include/                        # on PYTHONPATH (/opt/airflow/include)
│   │       ├── landing.py                  # Landing key layout (BUCKET_PREFIX="landing"), TRANSACTION_TABLES, watermark_var() + land()
│   │       ├── reconcile.py                # KEYS (PK per table), check_table(): OLTP (key, updated_at) vs Landing
│   │       ├── lakehouse_runner.py         # Docker Engine API exec_run helper driving olist-spark-master & olist-clickhouse
│   │       ├── oltp.py                     # engine() from Variable OLTP_URL
│   │       └── s3_utils.py                 # boto3 client, arrow_schema(), put_parquet(), put_json(), list_keys(), delete_keys(), read_parquet()
│   ├── .gitattributes                      # *.sh eol=lf (02_users.sh, minio-init.sh run in Linux containers)
│   ├── infra/minio/Dockerfile              # builds minio + mc from source (@latest — non-reproducible)
│   ├── infra/minio-init.sh                 # bucket + landing/ prefix + read-only lake-reader user/policy
│   ├── README.md                           # run commands + lakehouse contract
│   ├── architecture.md, architecture_diagram.md, plan_source_ingestion.md  # design docs (partly outdated, see implementation_log §7)
│   ├── implementation_log.md               # what was built, verified counts, incidents
│   └── deployment.md                       # prod deploy (compose override, Caddy, Tailscale), reset procedure §7.4
├── olist-lakehouse/                        # Stack 2: Spark/Iceberg medallion + Trino + ClickHouse
│   ├── olist_pipeline.py                   # spark-submit entrypoint: --stage ddl|bronze|silver-entities|silver-transactions|silver|gold-market|gold-seller|gold
│   ├── run_lakehouse.bat                   # stage runner (Windows), orchestrates compose + spark-submit + clickhouse-client
│   ├── common/spark_utils.py               # spark_session (per-bucket S3A), apply_ddl, conform, latest(), norm_city/zip, write_table, read_bronze
│   ├── layers/
│   │   ├── bronze/create_bronze_tables.sql # iceberg.bronze.* DDL (all STRING + metadata)
│   │   ├── bronze/load_bronze.py           # hosting_server Landing Parquet (s3a://olist-lake/landing) -> iceberg.bronze.* (append-only, all snapshots)
│   │   ├── silver/create_silver_tables.sql # iceberg.silver.* 9 3NF entity tables + iceberg.quarantine.silver_rejected_records
│   │   ├── silver/quarantine.py            # build_rejects() + partition-scoped overwrite into iceberg.quarantine.silver_rejected_records
│   │   ├── silver/entities.py              # build_entities(): geolocation, categories, customers, sellers, products
│   │   ├── silver/transactions.py          # build_transactions(): orders, order_items, order_payments, order_reviews + DQ quarantine
│   │   ├── gold/market/                    # iceberg.gold.* (8 Kimball marts for serving/olist-eda): create_gold_tables.sql, config.py, build_marts.py, seeds/
│   │   ├── gold/seller/                    # iceberg.gold_seller.* (27 marts for serving/seller-dashboard): create_gold_tables.sql, build_marts.py
│   │   ├── clickhouse/market/              # init_market.sql + load_market.sql -> olist_gold (8 MergeTree + 9 views, __new -> EXCHANGE)
│   │   └── clickhouse/seller/              # init_seller.sql + load_seller.sql -> olist_gold_seller (27 MergeTree, __new -> EXCHANGE)
│   ├── infrastructure/
│   │   ├── docker-compose.yml              # catalog-postgres, minio, minio-init, iceberg-rest, spark-master/worker, trino, clickhouse(+init)
│   │   ├── .env.example                    # ports, creds, SOURCE_S3_* (bat copies to .env if missing)
│   │   ├── spark/Dockerfile, spark-defaults.conf  # Iceberg catalog "iceberg", S3A -> lakehouse MinIO (creds from env AWS_ACCESS_KEY_ID/SECRET)
│   │   ├── iceberg-rest/Dockerfile         # adds postgres JDBC + iceberg-aws-bundle 1.5.0
│   │   ├── trino/{config,node}.properties, jvm.config, catalog/iceberg.properties  # S3 creds ${ENV:AWS_ACCESS_KEY_ID/SECRET}
│   │   ├── clickhouse/config.xml           # named S3 collection "minio" from env; timezone UTC
│   │   ├── clickhouse/users.xml            # NOT mounted by compose (dead config)
│   │   ├── minio/init-buckets.sh           # creates lakehouse; DELETES legacy buckets olist, warehouse
│   │   └── postgres/init.sql               # uuid-ossp extension
│   ├── docs/GOLD_DESIGN.md                 # Gold mart spec, reconciliation notes, status (2026-11-14)
│   ├── trino_queries.ipynb                 # ad-hoc Trino exploration (Silver 3NF + Quarantine + Gold)
│   └── README.md                           # stages, MinIO layout, ports (Vietnamese)
└── serving/                                # Unified Consumption / Serving Layer (2 branches)
    ├── README.md                           # overview + quickstart for both serving apps
    ├── olist-eda/                          # Branch 1: Market Analytics & Executive BI
    │   ├── eda/                            # 5 notebooks (01_executive..05_seller) querying Silver 3NF via Trino + reconciling iceberg.gold
    │   └── powerbi/                        # PBIP (17 semantic model tables, 5 pages), M GoldTable(view) -> ClickHouse olist_gold :8123
    └── seller-dashboard/                   # Branch 2: Seller Intelligence Dashboard
        ├── Seller_dashboard.py             # Streamlit + Plotly (5 pages) querying ClickHouse olist_gold_seller :8123
        ├── .env.example                    # ClickHouse connection template (never commit .env)
        └── requirements.txt                # Python dependencies
```

---

## 3. Core Architecture & End-to-End Flows

### 3.0 Topology

```mermaid
flowchart LR
  CSV[hosting_server/Data/*.csv] --> BE[build_events.py] --> EV[(events.parquet)]
  CSV --> SEED[seed.py] --> MY[(oltp-mysql :3307 db shop)]
  EV --> SIM[run.py bootstrap/live] --> MY
  MY -- etl_ro, updated_at window --> AF1[DAG ingest_oltp_to_landing */15]
  MY -- etl_ro, full scan --> AF2[DAG ingest_reference_snapshot @daily]
  AF1 & AF2 --> SL[(MinIO :9000 bucket olist-lake/landing)]
  SL -- lakehouse_reader via host.docker.internal:9000 --> SP[Spark olist_pipeline.py via DAG etl_lakehouse_medallion]
  SP --> ICE[(Iceberg on MinIO :9100 bucket lakehouse; REST catalog :8181)]
  ICE --> TR[Trino :8085] --> EDA[serving/olist-eda notebooks]
  SP -- export parquet --> EXP[(s3a://lakehouse/exports/gold & gold_seller)]
  EXP -- s3() --> CH[(ClickHouse :8123 olist_gold & olist_gold_seller)]
  CH -- HTTP CSVWithNames --> PBI[serving/olist-eda Power BI]
  CH -- clickhouse-connect --> ST[serving/seller-dashboard Streamlit]
```

Two separate MinIO instances: **source lake** (`hosting_server`, bucket `olist-lake`, host port 9000) and
**lakehouse** (bucket `lakehouse`, host port `EXTERNAL_MINIO_API_PORT`=9100, in-network `minio:9000`).
Spark reaches the source lake through the Docker host (`host.docker.internal:9000`, `extra_hosts: host-gateway`).

### 3.1 Initialization sequence

hosting_server (`docker compose up -d --build` in `hosting_server/`):
1. `oltp-mysql` starts; on an **empty** `oltp_data` volume runs `01_schema.sql` then `02_users.sh`.
2. `minio` healthy → `minio-init` runs `infra/minio-init.sh`: `mc mb olist-lake`, writes `.keep` in `landing/`, creates `LAKE_READER_*` user with policy `lake-reader` (`s3:GetBucketLocation`, `s3:ListBucket`, `s3:GetObject` on the bucket).
3. `airflow-postgres` healthy → `airflow-init` (`airflow db migrate`, create admin user) → `airflow-webserver` (:8080) + `airflow-scheduler`.
4. Airflow Variables are injected via env `AIRFLOW_VAR_OLTP_URL`, `AIRFLOW_VAR_MINIO_ENDPOINT` (`http://minio:9000`), `AIRFLOW_VAR_MINIO_ACCESS_KEY/SECRET_KEY/BUCKET`. Watermarks `wm__<table>` are real (DB-stored) Variables created at first commit.
5. One-time data prep in `simulator` (profile `sim`): `seed.py` → `build_events.py` → `run.py bootstrap --until 2017-06-01`.
6. DAGs are created paused (Airflow default) — must be unpaused in UI.

olist-lakehouse (`run_lakehouse.bat` stage `infra`): copies `.env.example`→`.env` if missing, `docker compose up -d --build`,
waits for `olist-minio`, `olist-iceberg-rest`, `olist-spark-master`, `olist-trino`, `olist-clickhouse` (healthy/running, 36×5 s).
Compose order: catalog-postgres + minio → minio-init (buckets) → iceberg-rest → spark-master → spark-worker; trino; clickhouse → clickhouse-init (`market/init_market.sql` + `seller/init_seller.sql`).

### 3.2 Flow A — Simulated order lifecycle (CSV → events → OLTP)

`build_events.py:main`:
1. Emits one row per event `(ts, kind, order_id, status, review_idx)`. Kinds in priority order `EVENT_KINDS = PURCHASE, APPROVED, SHIPPED, DELIVERED, REVIEW, TERMINAL` (`olist_data.py`).
2. `MILESTONES`: APPROVED→`order_approved_at`/`approved`, SHIPPED→`order_delivered_carrier_date`/`shipped`, DELIVERED→`order_delivered_customer_date`/`delivered`. Delivered orders with NULL delivery date use `order_estimated_delivery_date` as event ts (column itself stays NULL).
3. Every event ts is clamped to `>=` purchase ts.
4. `simulate_status` replays rank-guarded transitions; orders whose final status ≠ CSV get a forced `TERMINAL` event at `max(ts)+1s` (covers `canceled`, `unavailable`, `invoiced`, `processing`, …).
5. Final sort `(ts, prio, order_id)`; asserts: one PURCHASE per order, review count == CSV, final status == CSV, no event before purchase. Expected: 493,937 events.

`run.py` → `replay_until(conn, rp, events, start_ts, until, speed, status)`:
```
lo = searchsorted(start_ts, side=right); hi = searchsorted(until, side=right)
for each CHUNK=5000 slice:  Replayer.apply(cur, slice)  # per kind in KIND_ORDER
                            save_state(sim_state.id=1, virtual_ts=last ts or until)
                            COMMIT                       # data + clock in one transaction
```
`Replayer` handlers (`replay.py`): `_purchase` INSERT IGNORE customers, orders (status `created`, milestone dates NULL),
order_items, order_payments; `_approved/_shipped/_delivered` → `_set_field`: `UPDATE orders SET <col>=csv_value,
order_status = IF(FIELD(cur) < FIELD(target), target, cur)` (forward-only); `_review` INSERT IGNORE by CSV row index;
`_terminal` unconditional status UPDATE.
`live`: polls `sim_state.status` every 1 s; `paused` re-anchors `VirtualClock` (paused wall time is not replayed); exits when virtual time passes last event. `--speed 1440` = 1 real min ↔ 1 virtual day.

### 3.3 Flow B — Incremental ingestion (`ingest_oltp_to_landing`, every 15 min)

```
get_window ──► extract_load.expand(table ∈ TABLES) ──► write_manifest ──► commit_watermark (outlet Dataset s3://olist-lake/landing/)
```
1. `get_window`: `wm_to = SELECT NOW(3) - INTERVAL 10 SECOND` (DB clock, `LAG_SECONDS`); `wm_from = Variable wm__<t>` default `1970-01-01 00:00:00`. Same `wm_to` for all 5 tables.
2. `extract_load(table)`: `SELECT * FROM <t> WHERE updated_at > :a AND updated_at <= :b ORDER BY updated_at`, streamed `chunksize=100_000`; via `landing.land()` appends `_ingested_at`, `_source_table`, `_batch_id=stamp(wm_from)`, `_op='upsert'`; writes `landing/<t>/ingest_date=<wm_to YYYYMMDD>/batch_id=<stamp(wm_from)>/part-NNNNN.parquet` with fixed `arrow_schema` (snappy, µs timestamps). After writing, `land()` deletes any other object in that prefix (parts left by an earlier attempt that produced more chunks).
3. `write_manifest`: `landing/_manifests/batch_id=<stamp(wm_to)>/manifest.json` (`{batch_id, tables[{table,rows,parts,stale_parts_removed,from,to}], total_rows}`). Note: manifest key uses `wm_to`, data keys use `wm_from`.
4. `commit_watermark`: sets every `wm__<t>` = `wm_to` **only after** all tables + manifest succeeded → failed runs re-read the same window and overwrite the same keys.
Settings: `catchup=False`, `max_active_runs=1`, `retries=3`, `retry_delay=1m`.

`ingest_reference_snapshot` (@daily): for `products, sellers, geolocation, product_category_name_translation` full `SELECT *`
(chunk 200k) → `landing/ref_{products,sellers,geolocation,category_translation}/snapshot_date=<ds>/part-NNNNN.parquet`, metadata `_ingested_at, _source_table, _snapshot_date`.

`reconcile_landing` (@hourly, `max_active_runs=1`, `retries=1`): `check.expand(table ∈ TRANSACTION_TABLES)`. Per table: reads committed `wm__<t>` (skips if unset), `SELECT <PK>, updated_at FROM <t> WHERE updated_at <= wm` from OLTP, reads PK + `updated_at` of every Parquet object under `landing/<t>/`, compares `(PK, updated_at)` at ms precision. Any OLTP version missing in Landing → `AirflowFailException` (no retry) with count, sample keys and the repair command `airflow variables set wm__<t> '<oldest missing updated_at − 1 ms>'` (next ingest run re-lands from there; duplicate versions are deduped by Silver). Separate DAG because it reads all batches of a table (one small empty Parquet per table is written every 15-min run even without changes).

### 3.4 Flow C — Lakehouse batch (Airflow DAG `etl_lakehouse_medallion` or `run_lakehouse.bat` → `olist_pipeline.py`)

DAG `etl_lakehouse_medallion` (`hosting_server/airflow/dags/etl_lakehouse_medallion.py`): schedule `5 * * * *` (hourly at :05 UTC, after the `*/15` ingest), `catchup=False`, `max_active_runs=1`, `retries=1`.
`detect_manifest` finds the newest `landing/_manifests/` object with `total_rows > 0`; `check_new_data` (ShortCircuit) skips a scheduled run unless that manifest differs from Variable `lakehouse__last_manifest_batch`. Manual runs (or `dag_run.conf` `{"force": true}`) always run.
Tasks: `medallion_core` (`ddl → bronze → silver-entities → silver-transactions`), then `serving_market` (`gold-market → clickhouse market`) and `serving_seller` (`gold-seller → clickhouse seller`) in parallel, then `commit_etl_cursor` writes the Variable. Spark tasks use pool `spark_pool` (1 slot); `dag_run.conf` key `bronze_mode` (default `append`) sets the Bronze `--mode`. `lakehouse_runner.py` runs each stage by `docker exec` in `olist-spark-master` / `olist-clickhouse`.

`run_lakehouse.bat` (manual): stage order is fixed regardless of CLI order: `infra → ddl → bronze → silver-entities → silver-transactions → gold-market → gold-seller → clickhouse-market → clickhouse-seller`.
Aliases: `silver` (entities + transactions), `gold` (market + seller), `clickhouse` (market + seller), `all`. No args = all. Each Spark stage:
```
docker exec olist-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client \
  /opt/spark/olist_pipeline.py --stage <s> --layers-dir /opt/spark/layers --mode <OLIST_BRONZE_MODE|append> \
  --source-uri <OLIST_SOURCE_URI|s3a://olist-lake/landing> [--batch-id] [--start-date] [--end-date]
```
1. **ddl** `apply_ddl`: runs `bronze/create_bronze_tables.sql`, `silver/create_silver_tables.sql`, `gold/market/create_gold_tables.sql`, `gold/seller/create_gold_tables.sql` via `split_sql` (strips `--` comments naively, splits on `;`). All `CREATE TABLE IF NOT EXISTS` (gold also DROPs 3 legacy marts).
2. **bronze** `load_bronze`: per `SOURCES` entry: `spark.read.parquet(<uri>/<dataset>/ingest_date=*/batch_id=*/*.parquet)` (incremental) or `<uri>/<dataset>/snapshot_date=*/*.parquet` (snapshot, every snapshot_date kept); optional `ingest_date` filter from `--start-date/--end-date` (Airflow landing date, NOT business date); every column → STRING (`as_bronze_string`: timestamps `yyyy-MM-dd HH:mm:ss`); `updated_at` → `source_updated_at` (`yyyy-MM-dd HH:mm:ss.SSS`; reference tables get `snapshot_date 00:00:00.000`); adds `source_file`, `source_file_mtime` (Spark `_metadata.file_modification_time`), `loaded_at`, `batch_id`. Default `--mode append`: `filter_unloaded_files` drops landing files already in Bronze (incremental tables match `source_file` + `source_file_mtime`, snapshot tables match `source_file` only), then `.append()`. An incremental file rewritten by an ingest retry has a newer mtime and is loaded again; a rewritten snapshot file is not, because geolocation rows have no key and would be duplicated; rows with NULL mtime (loaded before the column existed) count as loaded. `--mode overwrite` uses `overwritePartitions()` (unpartitioned ⇒ full table replace). **All landed versions of every row are kept.** `ensure_metadata_columns` ALTERs existing tables to add `source_updated_at` and `source_file_mtime`.
3. **silver-entities** `build_entities` (`layers/silver/entities.py`; reads `read_bronze` = `blank_to_null(iceberg.bronze.<t>)`): geolocation, categories, customers, sellers, products; rejects go to quarantine. See §4.3.
4. **silver-transactions** `build_transactions` (`layers/silver/transactions.py`): orders, order_items, order_payments, order_reviews; dedups with `latest()`, applies orphan filters + timeline rules A1–A6 with quarantine, raises if `silver.orders` is empty.
5. **gold-market** `build_marts` (`layers/gold/market/`): `compute_marts(spark, silver, materialize)` builds the 8 Market marts (`iceberg.gold.*`); `materialize` = `conform()` to DDL schema, `writeTo(t).overwrite(lit(True))`, re-read table (cuts lineage), raise if 0 rows. Then `export_table` writes each to `s3a://lakehouse/exports/gold/<t>` (Parquet, overwrite). **gold-seller** (`layers/gold/seller/build_marts.py`) does the same for the 27 `iceberg.gold_seller.*` marts, exported to `s3a://lakehouse/exports/gold_seller/<t>`.
6. **clickhouse-market / clickhouse-seller**: `clickhouse-client` authenticates with `CLICKHOUSE_PASSWORD` from the container environment (set by compose from `infrastructure/.env`). Per branch: `init_<branch>.sql` (recreates `<t>__new` staging tables), then `load_<branch>.sql`: loads all `<t>__new` tables (8 for market, 27 for seller), then `EXCHANGE TABLES` each into `<t>`. Files are `layers/clickhouse/market/{init_market,load_market}.sql` and `layers/clickhouse/seller/{init_seller,load_seller}.sql`, mounted at `/opt/olist/clickhouse`. A failed load leaves the serving tables untouched.

### 3.5 Flow D — Consumption

- **Power BI**: `GoldTable(view)` M function → `GET {ClickHouseUrl}?query=SELECT * FROM olist_gold.<view> FORMAT CSVWithNames&format_csv_null_representation=` with headers `X-ClickHouse-User/Key`; empty string → null. Import mode only (DAX uses TOPN/RANKX/ADDCOLUMNS). View names = model table names: `order_summary, item_sales, item_flow, geo_customer, geo_seller, customer_rfm, first_order_repurchase, v_dim_seller (→ model dim_seller), dim_date, dim_state, dim_category`.
- **EDA**: `common.connect(tables)` (`serving/olist-eda/eda/common/tables.py`) → Trino (`TRINO_HOST=localhost`, `TRINO_PORT=8085`, `TRINO_USER=eda`, `TRINO_CATALOG=iceberg`, `SILVER_SCHEMA=silver`, `EDA_SCHEMA=eda`), CTAS-copies `BASE_TABLES=[orders, order_items, order_reviews, customers]` + requested Silver tables into `iceberg.eda` (TIMESTAMP WITH TZ → TIMESTAMP(6) UTC), creates `state_ref`, `order_base`, derives `Period` (same 10% rule as Gold; YoY + QoQ like DAX; override with env `REPORT_START`/`REPORT_END=YYYY-MM`, same year).

---

## 4. Data Models & Storage Layer

### 4.1 OLTP MySQL (`hosting_server/source/sql/01_schema.sql`, DB `OLTP_DATABASE`=`shop`)

| Table | PK / keys | Notes |
|---|---|---|
| `customers` | PK `customer_id`; idx `customer_unique_id`, `updated_at` | `customer_id` is per-order; `customer_unique_id` is the person |
| `orders` | PK `order_id`; FK `customer_id → customers` (only FK in schema) | `order_status` VARCHAR(16); 5 business DATETIMEs |
| `order_items` | PK `(order_id, order_item_id)` | DECIMAL(10,2) `price`, `freight_value` |
| `order_payments` | PK `(order_id, payment_sequential)` | |
| `order_reviews` | PK surrogate `review_pk` AUTO_INCREMENT; UNIQUE `(review_id, order_id)` | `review_id` not unique in CSV (814 dup rows) |
| `products`, `sellers`, `geolocation` (PK `geo_pk` auto, idx zip), `product_category_name_translation` | | reference; no `updated_at`; columns keep CSV typos `product_name_lenght`, `product_description_lenght` |
| `sim_state` | PK `id`=1 | `virtual_ts`, `speed`, `status` ∈ {`running`,`paused`} |

All 5 transaction tables have `created_at`, `updated_at TIMESTAMP(3) DEFAULT/ON UPDATE CURRENT_TIMESTAMP(3)` (wall clock, indexed `idx_upd`) — the CDC watermark column. Business timestamps keep CSV values.
Order status state machine (forward-only via SQL `FIELD` rank): `created → approved → (invoiced → processing →) shipped → delivered`; `TERMINAL` can set any status incl. `canceled`, `unavailable`, `invoiced`, `processing`.
Expected full-replay counts: customers 99,441 · orders 99,441 · order_items 112,650 · order_payments 103,886 · order_reviews 99,224 · products 32,951 · sellers 3,095 · geolocation 1,000,163 · translation 71.

### 4.2 Landing Zone layout (MinIO `olist-lake`)

```
landing/<customers|orders|order_items|order_payments|order_reviews>/ingest_date=YYYYMMDD/batch_id=<YYYYMMDDTHHMMSSffffff>/part-00000.parquet
landing/ref_<products|sellers|geolocation|category_translation>/snapshot_date=YYYY-MM-DD/part-00000.parquet
landing/_manifests/batch_id=<stamp(wm_to)>/manifest.json
```
Parquet schema per table = OLTP columns in `SELECT *` order mapped by `s3_utils._arrow_type` (DateTime/Date→`timestamp[us]`, Integer→int64, Numeric/Float→float64, else string) + metadata fields.

### 4.3 Iceberg (catalog `iceberg`, warehouse `s3a://lakehouse/`)

**Bronze** `iceberg.bronze.*` — append-only by default (idempotent on `source_file`, plus `source_file_mtime` for incremental tables), all business columns STRING + `source_updated_at STRING` (OLTP `updated_at` or `snapshot_date 00:00:00.000` for reference tables), `loaded_at TIMESTAMP, source_file STRING, batch_id STRING, source_file_mtime TIMESTAMP` (file modification time of the landing object); keeps all landed versions and all reference snapshots.
Tables: `olist_orders, olist_order_items, olist_order_payments, olist_order_reviews, olist_customers, olist_products, olist_sellers, olist_geolocation, product_category_name_translation`.

**Silver (3NF Entity Model)** `iceberg.silver.*` (natural keys only, no `xxhash64` surrogate keys, no `dim_date`, no aggregations):

| Table | Natural PK / FK | Derivation |
|---|---|---|
| `geolocation` | PK `zip_code_prefix` (INT) | filters latest Bronze `source_updated_at` snapshot; `min(norm_city)`, avg lat/lng, `source_row_count`; invalid zip -> Quarantine |
| `categories` | PK `product_category_name` | union products ∪ translation ∪ `CATEGORY_EN_HARDCODE` ∪ `'unknown'`; english fallback = PT name |
| `customers` | PK `customer_unique_id`; FK `zip_code_prefix` | 1/person; city/state/zip from row with max `customer_id`; null PK -> Quarantine |
| `sellers` | PK `seller_id`; FK `zip_code_prefix` | cleaned zip/city/state; null PK -> Quarantine |
| `products` | PK `product_id`; FK `product_category_name` | NULL category -> `'unknown'`; renames `*_lenght` -> `*_length`; null PK -> Quarantine |
| `orders` | PK `order_id`; FK `customer_unique_id` | resolves `customer_id -> customer_unique_id`; null PK, null `purchase_ts`, orphan customer, and A1–A6 timeline inversions -> Quarantine |
| `order_items` | PK `(order_id, order_item_id)`; FK `order_id`, `product_id`, `seller_id` | `item_total = price + freight_value`; orphan FKs -> Quarantine |
| `order_payments` | PK `(order_id, payment_sequential)`; FK `order_id` | orphan order -> Quarantine |
| `order_reviews` | PK `review_id` (dedup aligned with `GOLD_DESIGN.md`); FK `order_id` | `is_positive` (>=4), `is_negative` (<=2); orphan order -> Quarantine |

**Quarantine** `iceberg.quarantine.silver_rejected_records` (PARTITIONED BY `target_table`, overwritten per `target_table` on Silver rebuild): stores `quarantined_at, batch_id, source_table, target_table, record_key, rule_code, reason, source_updated_at, source_file, raw_payload` (JSON).

**Gold** (Kimball Star Schema / Data Marts; surrogate keys `xxhash64` generated here):
- **Market (`iceberg.gold.*`, 8 tables, `layers/gold/market/`)**: `fact_order_summary`, `fact_item_sales`, `report_period`, `dim_date`, `dim_state`, `dim_category`, `dim_seller`, `dim_customer_profile` -> exported to `s3a://lakehouse/exports/gold/` -> ClickHouse `olist_gold` (8 MergeTree + 9 views) -> `serving/olist-eda` Power BI & EDA reconciliation.
- **Seller (`iceberg.gold_seller.*`, 27 tables, `layers/gold/seller/`)**: `sales_detail`, `orders`, `payments_by_order`, `reviews`, `seller_order`, `customer_order_bridge`, daily/monthly sales/category/product/geo/status/delivery/hourly/weekday/review/customer marts -> exported to `s3a://lakehouse/exports/gold_seller/` -> ClickHouse `olist_gold_seller` (27 MergeTree) -> `serving/seller-dashboard` Streamlit app.

Gold labels/bands are Vietnamese strings consumed verbatim by Power BI (sort via `*_order` columns). Do not translate.

### 4.4 Lakehouse MinIO layout

`lakehouse/{bronze,silver,gold}/<table>/` (Iceberg data+metadata) · `lakehouse/exports/gold/<table>/*.parquet` (ClickHouse load source).

### 4.5 ClickHouse `olist_gold`

8 MergeTree tables mirroring Gold (same column order; `Nullable(...)` where Gold can be NULL; `LowCardinality` for labels). ORDER BY: `fact_order_summary (purchase_date, order_id)`, `fact_item_sales (purchase_date, order_id, order_item_id)`, `dim_date date`, `dim_state state`, `dim_category category`, `dim_seller seller_key`, `dim_customer_profile customer_key`, `report_period tuple()`.
Views for Power BI (names/columns = legacy CSV headers): `order_summary` (renames `order_gmv AS gmv`), `item_sales`, `item_flow`, `geo_customer`, `geo_seller`, `customer_rfm` (frequency>0), `first_order_repurchase` (observable only), `cust_2nd_gap` (days_to_2nd not null), `v_dim_seller`.
No caching layer / Redis anywhere. No indexes beyond MySQL keys and ClickHouse ORDER BY.

---

## 5. API & Interface Contracts

No HTTP API is implemented in this repo (FastAPI/CDC were planned, not built). Interfaces are CLIs, DAGs, storage paths and SQL objects.

| Interface | Contract |
|---|---|
| `python run.py bootstrap --until YYYY-MM-DD [--speed F]` | replays events `(sim_state.virtual_ts, until]`, leaves `status=paused` |
| `python run.py live [--speed 1440]` | requires existing `sim_state` row; runs until all events replayed |
| `python run.py pause|resume|status` | flips `sim_state.status` / prints state + 5 table counts |
| `python seed.py`, `build_events.py`, `verify_source.py` | idempotent seed; regenerate events; prints PASS/FAIL lines + `ALL PASS` |
| Airflow DAG `ingest_oltp_to_landing` | schedule `*/15 * * * *`; outlet Dataset `s3://olist-lake/landing/` (no consumer DAG exists) |
| Airflow DAG `ingest_reference_snapshot` | schedule `@daily`, keyed by logical date `ds` |
| Airflow DAG `reconcile_landing` | schedule `@hourly`; fails if an OLTP row version `<= wm__<t>` is absent from Landing; ad hoc: `airflow dags test reconcile_landing` |
| Airflow DAG `etl_lakehouse_medallion` | schedule `5 * * * *`; ShortCircuit on newest landing manifest vs Variable `lakehouse__last_manifest_batch` (manual runs always run); `dag_run.conf` `bronze_mode` (default `append`); pool `spark_pool`; runs `ddl → bronze → silver-entities → silver-transactions`, then Market and Seller branches (gold → ClickHouse) |
| Airflow Variables | `OLTP_URL`, `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET` (env-injected); `wm__customers|orders|order_items|order_payments|order_reviews` (state) |
| `olist_pipeline.py` | `--stage {ddl,bronze,silver-entities,silver-transactions,silver,gold-market,gold-seller,gold}` (required), `--layers-dir` (required), `--mode {overwrite,append}` (default `append`), `--source-uri` (default `s3a://olist-lake/landing`), `--start-date`, `--end-date`, `--batch-id` (default `UTC%Y%m%dT%H%M%S-<8hex>`) |
| `run_lakehouse.bat [stage ...]` | stages: `infra ddl bronze silver-entities silver-transactions gold-market gold-seller clickhouse-market clickhouse-seller`; aliases `silver`, `gold`, `clickhouse`, `all`, `help`; env `OLIST_SOURCE_URI`, `OLIST_BRONZE_MODE`, `OLIST_BATCH_ID`, `OLIST_START_DATE`, `OLIST_END_DATE` |
| Bronze Parquet contract (hosting → lakehouse) | fixed schema per table across batches; incremental tables carry `updated_at`; key layout §4.2; reader user needs `ListBucket` |
| Gold → ClickHouse contract | Iceberg DDL column order == export Parquet order == ClickHouse table column order (`INSERT ... SELECT *` is positional) |
| ClickHouse → Power BI contract | view names + column names in §4.5; numeric `customer_key` |

Error handling conventions: no global middleware. Scripts fail fast with exceptions and non-zero exit; the bat stops on first failing stage (`exit /b 1`). Spark stages raise `FileNotFoundError` (missing/empty landing data, with hint which DAG to run), `RuntimeError` (empty `silver.orders`, empty Gold table, no valid orders), `ValueError` (Gold DataFrame missing DDL columns in `conform`). Data-quality issues (orphans, timeline inversions, null keys) go to `iceberg.quarantine.silver_rejected_records`. Airflow retries 3× per task. ClickHouse load uses `insert_null_as_default = 0` so NULL into non-Nullable column errors out.

---

## 6. Configuration, Build & Runtime

### 6.1 Environment variables

`hosting_server/.env` (template `.env.example`; real `.env` gitignored — currently present on disk with real values, never print/commit it):

| Var | Purpose |
|---|---|
| `OLTP_ROOT_PASSWORD`, `OLTP_DATABASE` (`shop`), `OLTP_PORT` (host, 3307) | MySQL container |
| `OLTP_SIM_USER/PASSWORD` (`sim_rw`) | simulator RW user (`db.connect`) |
| `OLTP_ETL_USER/PASSWORD` (`etl_ro`) | Airflow read-only user (builds `OLTP_URL`) |
| `OLTP_HOST` | simulator host (compose overrides to `oltp-mysql`, port 3306) |
| `MINIO_ACCESS_KEY/SECRET_KEY` | MinIO root creds (also Airflow S3 creds) |
| `MINIO_BUCKET` (`olist-lake`) | source lake bucket — DAG Dataset URI hard-codes `olist-lake` |
| `LAKE_READER_ACCESS_KEY/SECRET_KEY` | read-only user for lakehouse Spark |
| `AIRFLOW_FERNET_KEY`, `AIRFLOW_SECRET_KEY`, `AIRFLOW_PG_PASSWORD`, `AIRFLOW_ADMIN_USER/PASSWORD` | Airflow |
| `DATA_DIR` (`/data`), `EVENTS_PATH` (`/app/events.parquet`) | simulator paths (`run.py` ignores `EVENTS_PATH`, hard-codes `/app/events.parquet`) |
| `MINIO_ENDPOINT`, `MYSQL_*` | unused by compose (`MYSQL_*` = stale Aiven warehouse config) |

`olist-lakehouse/infrastructure/.env` (template `.env.example`):

| Var | Default | Purpose |
|---|---|---|
| `POSTGRES_USER/PASSWORD`, `EXTERNAL_POSTGRES_PORT` | iceberg/…/5433 | Iceberg JDBC catalog DB |
| `MINIO_ROOT_USER/PASSWORD` | admin/admin123 | lakehouse MinIO; single source: compose passes them as `AWS_ACCESS_KEY_ID/SECRET_ACCESS_KEY` to Iceberg REST, Spark, Trino, and as `MINIO_ROOT_*` to ClickHouse (`config.xml` `from_env`) |
| `EXTERNAL_MINIO_API_PORT/CONSOLE_PORT` | 9100/9101 | host ports (9000 is taken by hosting_server MinIO) |
| `EXTERNAL_ICEBERG_REST_PORT` | 8181 | |
| `EXTERNAL_SPARK_UI_PORT`, `SPARK_WORKER_CORES`, `SPARK_WORKER_MEMORY` | 8084, 4, 6g | |
| `EXTERNAL_TRINO_PORT` | 8085 | |
| `EXTERNAL_CLICKHOUSE_HTTP_PORT/NATIVE_PORT`, `CLICKHOUSE_PASSWORD` | 8123/9200/admin123 | also hard-coded in Power BI `expressions.tmdl` |
| `SOURCE_S3_ENDPOINT/BUCKET/ACCESS_KEY/SECRET_KEY` | `http://host.docker.internal:9000`, `olist-lake`, `lakehouse_reader`, — | must equal hosting_server `LAKE_READER_*` |

EDA: `TRINO_HOST, TRINO_PORT, TRINO_USER, TRINO_CATALOG, SILVER_SCHEMA, EDA_SCHEMA`.

### 6.2 Commands

hosting_server (run in `hosting_server/`):
```bash
cp .env.example .env                       # fill values
docker compose up -d --build
docker compose --profile sim run --rm simulator python seed.py
docker compose --profile sim run --rm simulator python build_events.py
docker compose --profile sim run --rm simulator python run.py bootstrap --until 2017-06-01
docker compose --profile sim run -d --name sim-live simulator python run.py live --speed 1440
docker compose --profile sim run --rm simulator python run.py status
# unpause DAGs in http://localhost:8080 ; MinIO console http://localhost:9001
```
Verification (fresh DB only — it truncates nothing):
```bash
docker compose --profile sim run --rm simulator python run.py bootstrap --until 2018-12-31
docker compose --profile sim run --rm simulator python verify_source.py
```
Reset Bronze + watermarks: `hosting_server/deployment.md` §7.4 (delete non-`ref_` objects under `landing/`, `airflow variables delete wm__<t>`).
Schema change in OLTP: `01_schema.sql` only runs on an empty volume → `docker compose down -v` (destroys data) or apply DDL manually.

olist-lakehouse (PowerShell/cmd in `olist-lakehouse/`, Docker Desktop running, hosting_server DAGs run at least once):
```powershell
.\run_lakehouse.bat                              # infra ddl bronze silver gold clickhouse
.\run_lakehouse.bat ddl bronze silver gold clickhouse   # refresh, infra already up
.\run_lakehouse.bat ddl gold clickhouse          # after Gold logic/DDL change
.\run_lakehouse.bat clickhouse                   # reload ClickHouse from existing exports
```
Linux/macOS equivalent of a stage: `docker exec olist-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark/olist_pipeline.py --stage gold-market --layers-dir /opt/spark/layers --mode append --source-uri s3a://olist-lake/landing`.
UIs: Spark :8084, Trino :8085, Iceberg REST :8181, ClickHouse :8123, MinIO :9101.

Tests: none automated. Verification = `verify_source.py`, Spark stage assertions, `docs/GOLD_DESIGN.md` §5.2 reconciliation checklist, and running the 5 EDA notebooks end-to-end (their Gold reconciliation cells assert Silver ≙ Gold). `compute_marts()` is designed to be unit-testable with a local SparkSession (pass a dict of Silver DataFrames and an identity/caching `materialize`).
Deploy: `hosting_server/deployment.md` (compose override `docker-compose.prod.yml`, Caddy, Tailscale) — those override files are not in the repo.

---

## 7. Coding Conventions & Critical Gotchas

### 7.1 Conventions
- Python: small modules, module-level constants in UPPER_CASE, `from __future__ import annotations` in lakehouse, type hints on public functions, f-string SQL with table names from constant whitelists only. Values are always bound params (`%s` pymysql, `:a` SQLAlchemy).
- Idempotency everywhere in the source: `INSERT IGNORE`, rank-guarded `UPDATE`, deterministic S3 keys (overwrite on retry), watermark committed last.
- Transactions: simulator commits per 5000-event chunk together with `sim_state` (`replay_until`). Airflow has no transaction across tables; consistency comes from shared `wm_to` + late watermark commit.
- Spark: Bronze stays all-STRING; typing happens in Silver (`to_timestamp`, `.cast`). Use `common.spark_utils.read_bronze` (applies `blank_to_null`) and `latest(df, keys)` (orders by `source_updated_at` desc then `loaded_at` desc) for every Bronze read. Surrogate keys via `F.xxhash64(natural keys)` — keep identical expressions across tables so joins line up.
- Silver writes via `write_table` (cache → count → `overwritePartitions`). Gold writes via `materialize` (`overwrite(lit(True))`); do **not** use `write_table` in Gold (snapshot columns change on all rows).
- All Gold thresholds/bands/labels live in `layers/gold/market/config.py`; mirror any business-rule change in `serving/olist-eda/eda/common/` (`config.py`, `rfm.py`: `LOST_STATUSES`, `REPORT_MONTH_MIN_SHARE`, `MIN_SELLER_ORDERS`, RFM rules, `CATEGORY_SQL`) — they are duplicated by design.
- Logging = `print` (Spark driver stdout / Airflow task logs). No logger config.
- Timezone UTC end-to-end: MySQL `--default-time-zone=+00:00`, `spark.sql.session.timeZone=UTC`, ClickHouse `<timezone>UTC`, Trino session `timezone="UTC"`.
- Shell scripts mounted into Linux containers must stay LF (`*.sh text eol=lf` in both `olist-lakehouse/.gitattributes` and `hosting_server/.gitattributes`); with CRLF (`core.autocrlf=true` on Windows) `02_users.sh` stops `oltp-mysql` (`/bin/bash^M: bad interpreter`) and `minio-init` fails (`set: illegal option -`). `run_lakehouse.bat` itself must stay CRLF (`*.bat text eol=crlf`).

### 7.2 Invariants — do not break
1. **Parquet schema stability**: every batch of a Bronze table must use `s3_utils.arrow_schema(...)`. Without it an all-NULL column (e.g. `order_delivered_customer_date` of new orders) is written as Arrow `null` type and Spark cannot read the table's batches together. Money columns stay float64 (`coerce_float`).
2. **Timestamps must be µs** (`coerce_timestamps="us"`); Spark cannot read ns Parquet.
3. **Bronze `batch_id` = `stamp(wm_from)`** (`include/landing.py`), not `data_interval_end`. Using interval-based keys caused overlapping scheduled/manual runs to overwrite each other's data after the watermark advanced (real incident, `implementation_log.md` §6).
4. **Watermark commit must stay the last task** (`commit_watermark` depends on `write_manifest` depends on all mapped `extract_load`). Never set `wm__*` inside `extract_load`.
5. `LAG_SECONDS=10` window end uses the **DB clock** (`NOW(3)`), because `updated_at` is DB-generated. Don't switch to Airflow worker time.
6. **Column order is a cross-system contract**: `layers/gold/{market,seller}/create_gold_tables.sql` ↔ export Parquet ↔ `layers/clickhouse/{market/init_market,seller/init_seller}.sql` (positional `SELECT *` in `load_market.sql` / `load_seller.sql`). Adding/reordering a Gold column requires editing both DDLs, `build_marts.py`, and (if exposed) the ClickHouse view + Power BI `tables/*.tmdl`.
7. Gold/Silver/Bronze DDL is `CREATE TABLE IF NOT EXISTS` → changing an existing table's columns needs `DROP TABLE iceberg.<layer>.<t>` (or `ALTER`) before `ddl`; `conform()` will raise on missing columns. Bronze only auto-migrates `source_updated_at` and `source_file_mtime` (`ensure_metadata_columns`).
8. ClickHouse staging tables `<t>__new` are recreated every `clickhouse-market` / `clickhouse-seller` stage (`CREATE OR REPLACE`) and swapped into the serving tables with `EXCHANGE TABLES` only after all loads of that branch succeed (8 market, 27 seller); serving tables are created only if missing, so `clickhouse-init` at compose up no longer empties them. A view that references a newly added column fails until the serving table has the new schema: `DROP TABLE olist_gold.<t>` once, then rerun the `clickhouse` stage. Running `clickhouse` without `gold` reloads stale exports.
9. `events.parquet` stores `review_idx` = row index of `olist_order_reviews_dataset.csv`; `Replayer._review` re-reads the CSV and uses `.loc[review_idx]`. Never reorder/filter/edit the CSVs in `Data/` without regenerating `events.parquet` and rebuilding the OLTP DB.
10. `build_events.py` and `replay.py` must agree on milestone columns and status rank; `replay.py` re-declares `STATUS_ORDER`, `KIND_ORDER`, `ORDER_DATES`, `py()`, `rows()` instead of importing `olist_data`/`db` (duplication — change both).
11. Silver `silver.orders` must be built before items/payments/reviews (they read it back from the table); `silver-entities` must precede `silver-transactions`; `ddl` must precede everything on a fresh catalog.
12. `report_period` / `Period` are derived from data (latest month with ≥10% of peak valid orders) — never hard-code years/months in Gold, EDA or DAX.

### 7.3 Known gotchas & technical debt
- **Bronze "overwrite" replaces the whole table** (unpartitioned + `overwritePartitions`). `olist_pipeline.parse_args` therefore rejects `--stage bronze` with `--start-date/--end-date` unless `--mode append`. Use `OLIST_BRONZE_MODE=append` for partial loads (duplicates are tolerated by `latest()`).
- Date filters for `hosting` source refer to `ingest_date` (Airflow landing day of `wm_to`), not order purchase dates.
- Retries of the same window/snapshot rewrite the same prefix and `land()` removes leftover parts. A failed run re-done by a *new* DAG run keeps `wm_from` but gets a new `wm_to`, so if the day changed its batch lands under a different `ingest_date=` and the partial files of the failed run stay (duplicate row versions, harmless because Silver dedups).
- Watermark gap: `updated_at` is the statement time, not the commit time. A transaction committing more than `LAG_SECONDS` (10 s) after its writes leaves rows behind an already-read window; they are never extracted. Simulator chunks commit in ~0.7 s (full bootstrap 493,937 events in ~70 s), so this is unlikely, and `reconcile_landing` detects it.
- `ingest_reference_snapshot` keys by logical date `ds`; Bronze keeps every `snapshot_date`; `source_updated_at` is `snapshot_date 00:00:00.000`, so `latest()` picks the newest snapshot.
- `silver.order_reviews` dedups on `review_id` alone while OLTP uniqueness is `(review_id, order_id)`: 789 review_ids map to multiple orders; which order keeps the review depends on version order (~70 orders differ run-to-run). Changing the key changes Gold metrics — coordinate with `docs/GOLD_DESIGN.md`.
- Silver drops ~1,400 orders with inverted timelines (A1–A6) → Silver ≈ 98,059 orders vs 99,441 in OLTP; Gold `fact_item_sales` ≈ 110,500 rows. These are expected reference numbers for a full replay.
- `silver.customers` picks city/state from the max `customer_id` per person (deterministic but arbitrary).
- `silver.geolocation.zip_code_prefix` is INT (leading zeros dropped) and joins use `zip_as_int`; keep both sides INT.
- `common.spark_utils.split_sql` truncates any line at the first `--`, including inside string literals — never put `--` in DDL string values/comments-in-strings.
- Power BI parameter `ClickHousePassword` (`expressions.tmdl`) is hard-coded `admin123`; changing `CLICKHOUSE_PASSWORD` in `infrastructure/.env` requires updating it. Lakehouse MinIO/ClickHouse creds elsewhere come only from `.env`.
- `infrastructure/clickhouse/users.xml` is not mounted; the ClickHouse image's default user uses `CLICKHOUSE_PASSWORD`.
- `infrastructure/minio/init-buckets.sh` force-removes buckets named `olist` and `warehouse` on every start.
- `hosting_server/infra/minio/Dockerfile` uses `go install ...@latest` → rebuilds pull whatever MinIO master is current; `iceberg-rest-fixture:latest` is also unpinned.
- `run_lakehouse.bat` is Windows-only. The old crawler source (`olist-crawler-to-minio` / `olist-data-source-v2`, CSV in a `handoff` bucket) has been removed: no `datasource`/`crawl` stages, no `--source` flag, `handoff` bucket no longer created; `hosting_server` Parquet is the only Bronze source.
- `run.py` crash-resume edge case: `sim_state.virtual_ts` is saved as the last ts of a committed chunk and resume uses `searchsorted(side="right")`; events sharing that exact ts but falling in the next uncommitted chunk would be skipped after a crash mid-chunk-boundary.
- `run.py live` ignores `--speed` changes made via `sim_state.speed` (speed comes only from CLI).
- Dataset outlet `s3://olist-lake/landing/` has no consumer DAG; `etl_lakehouse_medallion` is time-scheduled (`5 * * * *`) and gated on the newest landing manifest instead.
- README claims Silver/Gold facts are month-partitioned; only Gold `fact_order_summary` and `fact_item_sales` are (`months(purchase_date)`). Silver tables are unpartitioned.
- `docs/GOLD_DESIGN.md` §4 mentions an ODBC connector; the model actually uses the HTTP `GoldTable` M function.
- Trino 440 bug: `FILTER (WHERE string_col = 'const')` throws `DictionaryBlock.getDictionary`; use `sum(CASE WHEN ... END)` in EDA SQL. Trino REST catalog has no views → EDA materializes tables in `iceberg.eda`; notebooks share table names, so run them sequentially or set distinct `EDA_SCHEMA`.
- Power BI tables `cust_2nd_gap` and `seller_review_bin` are DAX calculated tables even though a ClickHouse `cust_2nd_gap` view exists; `dim_seller` model table reads view `v_dim_seller` (n_categories/category_band are DAX calculated columns there).
- `.pbi/localSettings.json` and `.pbi/cache.abf` are gitignored artifacts; edit TMDL/PBIR JSON, not the cache.
- Do not commit `.env` files (both projects gitignore them; `hosting_server/.env` historically contained Aiven credentials).
