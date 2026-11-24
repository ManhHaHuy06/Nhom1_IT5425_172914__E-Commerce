"""Olist medallion pipeline entrypoint: Bronze (append) -> Silver (3NF + Quarantine) -> Gold (Market + Seller).

Thin orchestrator: parse CLI args and dispatch one stage to the layer modules under layers/.
"""

from __future__ import annotations

import argparse
import uuid
from datetime import datetime, timezone

from common.spark_utils import apply_ddl, spark_session
from layers.bronze.load_bronze import DEFAULT_SOURCE_URI, load_bronze
from layers.gold.market.build_marts import build_marts as build_gold_market
from layers.gold.seller.build_marts import build_gold as build_gold_seller
from layers.silver.entities import build_entities
from layers.silver.transactions import build_transactions


def _build_silver(spark, args) -> None:
    build_entities(spark, args.batch_id)
    build_transactions(spark, args.batch_id)


def _build_gold_all(spark, _args) -> None:
    build_gold_market(spark)
    build_gold_seller(spark)


STAGES = {
    "ddl": lambda spark, a: apply_ddl(spark, a.layers_dir),
    "bronze": lambda spark, a: load_bronze(
        spark, batch_id=a.batch_id, mode=a.mode, source_uri=a.source_uri,
        start_date=a.start_date, end_date=a.end_date,
    ),
    "silver-entities": lambda spark, a: build_entities(spark, a.batch_id),
    "silver-transactions": lambda spark, a: build_transactions(spark, a.batch_id),
    "silver": _build_silver,
    "gold-market": lambda spark, _a: build_gold_market(spark),
    "gold-seller": lambda spark, _a: build_gold_seller(spark),
    "gold": _build_gold_all,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Olist Iceberg medallion pipeline")
    parser.add_argument("--stage", required=True, choices=list(STAGES))
    parser.add_argument("--layers-dir", required=True, help="Directory with medallion layer packages (DDL + transforms)")
    parser.add_argument("--source-uri", default=DEFAULT_SOURCE_URI,
                        help=f"hosting_server Landing prefix (default {DEFAULT_SOURCE_URI})")
    parser.add_argument("--start-date", help="Inclusive YYYY-MM-DD filter on ingest_date of incremental batches")
    parser.add_argument("--end-date", help="Inclusive YYYY-MM-DD filter, same as --start-date")
    parser.add_argument("--batch-id", help="Batch id for Bronze load and Silver quarantine tracing")
    parser.add_argument("--mode", choices=["overwrite", "append"], default="append", help="Bronze write mode")
    args = parser.parse_args()
    # Bronze tables are unpartitioned: overwrite + date filter would keep only that ingest_date range.
    if args.stage == "bronze" and args.mode == "overwrite" and (args.start_date or args.end_date):
        parser.error("--start-date/--end-date require --mode append (OLIST_BRONZE_MODE=append); "
                     "overwrite replaces the whole Bronze table")
    args.batch_id = args.batch_id or (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8])
    return args


def main() -> None:
    args = parse_args()
    spark = spark_session()
    try:
        STAGES[args.stage](spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
