"""Refresh ClickHouse Gold through staged tables; never TRUNCATE live serving tables."""
from __future__ import annotations

import uuid
import requests
from airflow.models import Variable

GOLD_TABLES = (
    "sales_detail", "orders", "payments_by_order", "reviews", "seller_order",
    "customer_order_bridge", "sales_performance_daily", "sales_performance_monthly",
    "category_performance_daily", "category_performance_monthly", "product_performance_monthly",
    "geography_performance_daily", "geography_performance_monthly", "order_status_daily",
    "order_status_monthly", "delivery_performance_daily", "delivery_performance_monthly",
    "order_hourly_daily", "order_hourly_monthly", "order_weekday_monthly",
    "review_score_daily", "review_score_monthly", "review_geography_monthly",
    "review_category_monthly", "customer_lifetime", "customer_activity_monthly",
    "customer_activity_daily",
)


def refresh_clickhouse():
    endpoint = Variable.get("CLICKHOUSE_HTTP_URL").rstrip("/")
    auth = (
        Variable.get("CLICKHOUSE_USER", default_var="default"),
        Variable.get("CLICKHOUSE_PASSWORD"),
    )
    access = Variable.get("MINIO_ACCESS_KEY").replace("'", "''")
    secret = Variable.get("MINIO_SECRET_KEY").replace("'", "''")

    def execute(sql: str) -> str:
        response = requests.post(
            endpoint, params={"wait_end_of_query": "1"}, data=sql.encode("utf-8"),
            auth=auth, headers={"Content-Type": "text/plain; charset=utf-8"},
            timeout=(15, 21600),
        )
        if response.status_code != 200:
            detail = response.text.replace(access, "[REDACTED]").replace(secret, "[REDACTED]")
            raise RuntimeError(f"ClickHouse query failed ({response.status_code}): {detail[-6000:]}")
        return response.text.strip()

    # Require Atomic so each table swap is metadata-atomic. Fail before creating staging tables otherwise.
    db_engine = execute("SELECT engine FROM system.databases WHERE name='olist_gold' FORMAT TabSeparated")
    if db_engine != "Atomic":
        raise RuntimeError(
            f"olist_gold database engine is {db_engine or 'missing'}, expected Atomic for safe staged table exchange"
        )

    actual = set(filter(None, execute(
        "SELECT name FROM system.tables WHERE database='olist_gold' FORMAT TabSeparated"
    ).splitlines()))
    expected = set(GOLD_TABLES)
    if actual != expected:
        raise RuntimeError(
            "Existing ClickHouse olist_gold schema does not match Lakehouse Gold marts. "
            f"Missing={sorted(expected-actual)}, extra={sorted(actual-expected)}. No serving tables changed."
        )

    run_tag = uuid.uuid4().hex[:10]
    staged: dict[str, str] = {}
    exchange_started = False
    try:
        # Stage and validate every mart before touching any live table.
        for table in GOLD_TABLES:
            stage = f"__next_{table}_{run_tag}"
            staged[table] = stage
            execute(f"CREATE TABLE olist_gold.`{stage}` AS olist_gold.`{table}`")
            source = f"http://minio:9000/lakehouse/exports/gold/{table}/*.parquet"
            execute(
                f"INSERT INTO olist_gold.`{stage}` "
                f"SELECT * FROM s3('{source}', '{access}', '{secret}', 'Parquet')"
            )
            rows = int(execute(f"SELECT count() FROM olist_gold.`{stage}` FORMAT TabSeparated"))
            print(f"[CLICKHOUSE STAGE] {table}: {rows:,} rows", flush=True)

        # Each exchange is atomic for one table. Keep old live tables under staging names
        # until all exchanges complete; never truncate a live table.
        exchange_started = True
        for table in GOLD_TABLES:
            execute(f"EXCHANGE TABLES olist_gold.`{table}` AND olist_gold.`{staged[table]}`")

        # After all exchanges succeed, staging names point to the previous generation.
        for stage in staged.values():
            execute(f"DROP TABLE olist_gold.`{stage}`")
        print(f"[CLICKHOUSE] Atomically exchanged {len(GOLD_TABLES)} staged Gold tables", flush=True)
        return {"status": "success", "tables": len(GOLD_TABLES), "run_tag": run_tag}
    except Exception:
        if not exchange_started:
            # Safe to remove only our unexchanged staging generation; live tables remain untouched.
            for stage in staged.values():
                try:
                    execute(f"DROP TABLE IF EXISTS olist_gold.`{stage}`")
                except Exception:
                    pass
        else:
            # A failure during the sequence of exchanges can leave a mixed generation.
            # Preserve all stage tables for explicit recovery; never guess which generation to drop.
            print(
                f"[CLICKHOUSE RECOVERY REQUIRED] Exchange sequence incomplete. Preserve stage tables: {staged}",
                flush=True,
            )
        raise
