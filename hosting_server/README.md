# Olist pipeline – Source → Airflow → MinIO (Landing Zone) & Lakehouse ETL

Implements steps 1–9 of `plan_source_ingestion.md`. Design: `architecture.md`.

## Run from scratch
```bash
cp .env.example .env            # then fill in real values (never commit .env)
docker compose up -d --build    # oltp-mysql, minio (+bucket init), airflow (postgres, scheduler, webserver)

# one-time data prep (inside the simulator container; reads ./Data)
docker compose --profile sim run --rm simulator python seed.py                 # reference tables
docker compose --profile sim run --rm simulator python build_events.py        # source/events.parquet
docker compose --profile sim run --rm simulator python run.py bootstrap --until 2017-06-01

# start the "website": replays orders in virtual time (1 real minute = 1 virtual day)
docker compose --profile sim run -d --name sim-live simulator python run.py live --speed 1440   # stop: docker stop sim-live && docker rm sim-live
docker compose --profile sim run --rm simulator python run.py status
docker compose --profile sim run --rm simulator python run.py pause | resume
```
Airflow UI http://localhost:8080 (user/password: `AIRFLOW_ADMIN_*` in `.env`) – unpause
`ingest_oltp_to_landing` (every 15 min), `ingest_reference_snapshot` (daily), `reconcile_landing` (hourly check)
and `etl_lakehouse_medallion` (hourly cross-layer Lakehouse ETL).
MinIO console http://localhost:9001 · OLTP MySQL `localhost:3307`.

## Verify
```bash
# full replay then reconcile OLTP vs CSV (counts, status, timestamps, sums) – truncates nothing, so run on a fresh DB
docker compose --profile sim run --rm simulator python run.py bootstrap --until 2018-12-31
docker compose --profile sim run --rm simulator python verify_source.py
```
Landing completeness (DAG `reconcile_landing`, hourly; run now with
`docker compose exec airflow-scheduler airflow dags test reconcile_landing`): every OLTP row version with
`updated_at <= wm__<table>` must be in `landing/<table>/`. Rows can be skipped when a transaction commits more than
`LAG_SECONDS` after its writes (`updated_at` is the statement time). On failure the task message gives the
`airflow variables set wm__<table> '<ts>'` command that moves the watermark back so the next ingest run re-lands them.

## Downstream: olist-lakehouse
`../olist-lakehouse` reads this Landing Zone (`s3a://olist-lake/landing`) with Spark, through host port 9000, as the
read-only user `LAKE_READER_*` (created by `infra/minio-init.sh`). Copy that key pair into
`olist-lakehouse/infrastructure/.env` as `SOURCE_S3_ACCESS_KEY` / `SOURCE_S3_SECRET_KEY`, run both ingestion DAGs at least once,
then let `etl_lakehouse_medallion` schedule the lakehouse or run `olist-lakehouse/run_lakehouse.bat`. Contract between the two:
- Every Parquet batch of a table has the same schema (`arrow_schema` in `airflow/include/s3_utils.py`, derived from
  the OLTP columns), so Spark can read all batches together. Batches written before this change may have Arrow null
  columns: reset the watermarks (`wm__*` Variables) and clear `landing/` to re-ingest.
- Incremental tables keep `updated_at`; Silver keeps the newest version per key.

## Code layout
| File | Role |
|---|---|
| `source/olist_data.py` | CSV paths/readers + order-lifecycle constants (statuses, event kinds, milestone → column) shared by all simulator scripts |
| `source/db.py` | MySQL connection, NaN→NULL row conversion, `insert_ignore` |
| `source/build_events.py` | CSV → `events.parquet` (time-ordered replay events) |
| `source/replay.py` | applies an event slice to the OLTP DB (idempotent) |
| `source/run.py` | simulator CLI: bootstrap / live / pause / resume / status |
| `source/seed.py`, `source/verify_source.py` | seed reference tables; reconcile OLTP vs CSV (exit code 1 on failure) |
| `airflow/include/landing.py` | Landing Zone key layout, watermark tables/Variables + `land()` (stream a query to fixed-schema Parquet parts, then delete parts left by an earlier attempt) |
| `airflow/include/reconcile.py` | OLTP vs Landing version check used by `reconcile_landing` |
| `airflow/include/lakehouse_runner.py` | Docker Engine API runner used by `etl_lakehouse_medallion` to trigger Spark & ClickHouse stages |
| `airflow/include/oltp.py`, `s3_utils.py` | cached OLTP engine / S3 client (put/list/delete/read), Arrow schema from OLTP columns |

## Notes
- Official MinIO images/binaries are no longer published, so `infra/minio/Dockerfile` builds MinIO + mc from source.
- Landing keys use the window start (`wm_from`), see the docstring in `airflow/dags/ingest_oltp_to_landing.py`.
