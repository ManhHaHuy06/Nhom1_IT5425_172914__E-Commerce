# Olist pipeline – Source → Airflow → MinIO (Bronze)

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
`ingest_oltp_to_bronze` (every 15 min) and `ingest_reference_snapshot` (daily).
MinIO console http://localhost:9001 · OLTP MySQL `localhost:3307`.

## Verify
```bash
# full replay then reconcile OLTP vs CSV (counts, status, timestamps, sums) – truncates nothing, so run on a fresh DB
docker compose --profile sim run --rm simulator python run.py bootstrap --until 2018-12-31
docker compose --profile sim run --rm simulator python verify_source.py
```

## Notes
- Official MinIO images/binaries are no longer published, so `infra/minio/Dockerfile` builds MinIO + mc from source.
- Bronze keys use the window start (`wm_from`), see the docstring in `airflow/dags/ingest_oltp_to_bronze.py`.
