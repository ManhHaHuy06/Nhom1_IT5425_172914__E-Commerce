"""Authenticated Spark job runner using the Lakehouse Spark Standalone cluster."""
from __future__ import annotations

import os
import subprocess
import threading
from pathlib import Path
from typing import List, Literal, Optional

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

APP_ROOT = Path("/opt/olist")
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
TOKEN = os.environ["SPARK_RUNNER_TOKEN"]
MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
LOCK = threading.Lock()
app = FastAPI(title="Olist Lakehouse Spark Runner", docs_url=None, redoc_url=None)


class Job(BaseModel):
    stage: Literal["catalog-check", "ddl", "bronze-transactional", "bronze-reference",
                   "silver-dimensions", "silver-facts", "gold"]
    source_uri: Optional[str] = None
    batch_id: Optional[str] = None
    skip_tables: List[str] = Field(default_factory=list)



def spark_conf() -> list[str]:
    minio_endpoint = os.getenv(
        "LAKEHOUSE_MINIO_ENDPOINT",
        "http://minio:9000"
    )

    values = {
        # Spark networking
        "spark.driver.host": "spark-job-runner",
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.driver.port": "37001",
        "spark.blockManager.port": "37002",

        # Iceberg REST Catalog
        "spark.sql.extensions":
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.iceberg":
            "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg.type": "rest",
        "spark.sql.catalog.iceberg.uri": os.getenv(
            "LAKEHOUSE_ICEBERG_REST_URI",
            "http://iceberg-rest:8181"
        ),
        "spark.sql.catalog.iceberg.warehouse": "s3a://lakehouse/",

        # Iceberg S3FileIO
        "spark.sql.catalog.iceberg.io-impl":
            "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.iceberg.s3.endpoint": minio_endpoint,
        "spark.sql.catalog.iceberg.s3.path-style-access": "true",
        "spark.sql.catalog.iceberg.s3.region": "us-east-1",

        # Hadoop S3A
        "spark.hadoop.fs.s3a.endpoint": minio_endpoint,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl":
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider":
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",

        # Default catalog
        "spark.sql.defaultCatalog": "iceberg",

        # Performance
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.shuffle.partitions": os.getenv(
            "SPARK_SHUFFLE_PARTITIONS", "16"
        ),

        # Resources
        "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY", "2g"),
        "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
        "spark.executor.cores": os.getenv("SPARK_EXECUTOR_CORES", "2"),

        # Iceberg
        "spark.sql.iceberg.handle-timestamp-without-timezone": "true",
    }

    args: list[str] = []
    for key, value in values.items():
        args.extend(["--conf", f"{key}={value}"])

    return args



def command_for(job: Job) -> list[str]:
    common = [SPARK_SUBMIT, "--master", MASTER, "--deploy-mode", "client", *spark_conf()]
    if job.stage == "catalog-check":
        script = APP_ROOT / "spark_runner/catalog_smoke.py"
        args: list[str] = []
    elif job.stage == "ddl":
        script = APP_ROOT / "olist_pipeline.py"
        args = ["--stage", "ddl", "--layers-dir", str(APP_ROOT / "layers"), "--mode", "overwrite"]
    elif job.stage.startswith("bronze-"):
        if not job.source_uri or not job.batch_id:
            raise HTTPException(400, "Bronze jobs require source_uri and batch_id")
        mode = "transactional" if job.stage == "bronze-transactional" else "reference"
        script = APP_ROOT / "layers/bronze/load_oltp_bronze.py"
        args = ["--mode", mode, "--source-uri", job.source_uri, "--batch-id", job.batch_id]
        if job.skip_tables:
            args.extend(["--skip-tables", ",".join(job.skip_tables)])
    else:
        script = APP_ROOT / "olist_pipeline.py"
        args = ["--stage", job.stage, "--layers-dir", str(APP_ROOT / "layers"), "--mode", "overwrite"]
    return [*common, str(script), *args]


@app.get("/health")
def health():
    return {"status": "ok", "runtime": "shared-lakehouse-spark", "master": MASTER}


@app.post("/run")
def run_job(job: Job, x_runner_token: Optional[str] = Header(default=None)):
    if x_runner_token != TOKEN:
        raise HTTPException(401, "Invalid runner token")
    if not LOCK.acquire(blocking=False):
        raise HTTPException(409, "Another Spark job is running; retry this task")
    try:
        command = command_for(job)
        print(f"[SPARK RUNNER] stage={job.stage}; master={MASTER}; credentials redacted", flush=True)
        result = subprocess.run(
            command, cwd=str(APP_ROOT), text=True, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, timeout=int(os.getenv("SPARK_JOB_TIMEOUT_SECONDS", "21600")),
        )
        if result.stdout:
            print(result.stdout, flush=True)
        if result.returncode != 0:
            raise HTTPException(500, f"Spark job failed (exit={result.returncode}). See spark-job-runner logs.\n{result.stdout[-12000:]}")
        return {"status": "success", "stage": job.stage, "output": result.stdout[-12000:]}
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(504, "Spark job timed out; inspect spark-job-runner logs") from exc
    finally:
        LOCK.release()
