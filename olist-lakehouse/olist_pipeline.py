"""Olist medallion pipeline entrypoint: Bronze -> Silver -> Gold.

Thin orchestrator: parse CLI args and dispatch one stage to the layer modules under layers/.
"""

from __future__ import annotations

import argparse
import uuid
from datetime import datetime, timezone

from common.spark_utils import apply_ddl, spark_session
from layers.bronze.load_bronze import DEFAULT_SOURCE_URI, load_bronze
from layers.gold.build_marts import build_marts
from layers.silver.dimensions import build_dimensions
from layers.silver.facts import build_facts

STAGES = {
    "ddl": lambda spark, a: apply_ddl(spark, a.layers_dir),
    "bronze": lambda spark, a: load_bronze(spark, batch_id=a.batch_id, mode=a.mode, source_uri=a.source_uri,
                                           start_date=a.start_date, end_date=a.end_date),
    "silver-dimensions": lambda spark, a: build_dimensions(spark),
    "silver-facts": lambda spark, a: build_facts(spark),
    "gold": lambda spark, a: build_marts(spark),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Olist Iceberg medallion pipeline")
    parser.add_argument("--stage", required=True, choices=list(STAGES))
    parser.add_argument("--layers-dir", required=True, help="Directory with medallion layer packages (DDL + transforms)")
    parser.add_argument("--source-uri", default=DEFAULT_SOURCE_URI,
                        help=f"hosting_server Bronze prefix (default {DEFAULT_SOURCE_URI})")
    parser.add_argument("--start-date", help="Inclusive YYYY-MM-DD filter on ingest_date of incremental batches")
    parser.add_argument("--end-date", help="Inclusive YYYY-MM-DD filter, same as --start-date")
    parser.add_argument("--batch-id", help="Bronze batch_id (default: UTC timestamp + random suffix)")
    parser.add_argument("--mode", choices=["overwrite", "append"], default="overwrite", help="Bronze write mode")
    args = parser.parse_args()
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
