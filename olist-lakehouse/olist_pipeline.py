"""Olist medallion pipeline entrypoint: Bronze -> Silver -> Gold.

Thin orchestrator: parse CLI args and dispatch to layer modules under layers/.
"""

from __future__ import annotations

import argparse
import uuid
from datetime import datetime

from common.spark_utils import apply_ddl, spark_session
from layers.bronze.load_bronze import DEFAULT_SOURCE_URI, load_bronze
from layers.gold.build_marts import build_marts
from layers.silver.dimensions import build_dimensions
from layers.silver.facts import build_facts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Olist Iceberg medallion pipeline")
    parser.add_argument(
        "--stage",
        required=True,
        choices=["ddl", "bronze", "silver-dimensions", "silver-facts", "gold"],
    )
    parser.add_argument(
        "--layers-dir",
        required=True,
        help="Directory with medallion layer packages (DDL + transforms)",
    )
    parser.add_argument(
        "--source-uri",
        default=None,
        help="Bronze CSV handoff prefix, e.g. s3a://handoff/bronze",
    )
    parser.add_argument("--start-date", help="Inclusive YYYY-MM-DD filter for transaction paths")
    parser.add_argument("--end-date", help="Inclusive YYYY-MM-DD filter for transaction paths")
    parser.add_argument("--batch-id")
    parser.add_argument("--mode", choices=["overwrite", "append"], required=True)
    args = parser.parse_args()
    args.source_uri = args.source_uri or DEFAULT_SOURCE_URI
    return args


def main() -> None:
    args = parse_args()
    batch_id = args.batch_id or datetime.utcnow().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
    spark = spark_session()
    if args.stage == "ddl":
        apply_ddl(spark, args.layers_dir)
    elif args.stage == "bronze":
        load_bronze(
            spark,
            batch_id=batch_id,
            mode=args.mode,
            source_uri=args.source_uri,
            start_date=args.start_date,
            end_date=args.end_date,
        )
    elif args.stage == "silver-dimensions":
        build_dimensions(spark)
    elif args.stage == "silver-facts":
        build_facts(spark)
    elif args.stage == "gold":
        build_marts(spark)
    spark.stop()


if __name__ == "__main__":
    main()
