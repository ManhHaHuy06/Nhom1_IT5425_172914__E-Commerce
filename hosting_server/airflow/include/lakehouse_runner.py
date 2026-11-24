"""Docker Engine API runner used by Airflow DAG etl_lakehouse_medallion to drive Spark and ClickHouse stages."""

from __future__ import annotations

import json

import docker
from airflow.models import Variable

from landing import BUCKET_PREFIX
from s3_utils import bucket, client as s3_client, list_keys

VALID_SPARK_STAGES = frozenset({
    "ddl",
    "bronze",
    "silver-entities",
    "silver-transactions",
    "silver",
    "gold-market",
    "gold-seller",
    "gold",
})
VALID_CH_BRANCHES = frozenset({"market", "seller"})


def _exec_in_container(container_name: str, cmd: list[str], timeout_sec: int) -> None:
    """Run ``cmd`` inside a running container under wall-clock ``timeout``, stream logs, and fail on non-zero exit."""
    wrapped = ["timeout", "--signal=TERM", "--kill-after=30", str(timeout_sec), *cmd]
    # Add 60s headroom over container wall-clock timeout so TERM/KILL exit code is returned cleanly.
    docker_client = docker.from_env(timeout=timeout_sec + 60)
    try:
        container = docker_client.containers.get(container_name)
        container.reload()
        if container.status != "running":
            raise RuntimeError(f"Container {container_name!r} is not running (status={container.status})")
        exec_id = docker_client.api.exec_create(container.id, wrapped, stdout=True, stderr=True)["Id"]
        for chunk in docker_client.api.exec_start(exec_id, stream=True):
            print(chunk.decode("utf-8", errors="replace"), end="", flush=True)
        exit_code = docker_client.api.exec_inspect(exec_id)["ExitCode"]
        if exit_code != 0:
            raise RuntimeError(f"Command failed in {container_name!r} (exit={exit_code}): {' '.join(cmd)}")
    finally:
        docker_client.close()


def run_spark_stage(
    stage: str,
    mode: str = "append",
    source_uri: str | None = None,
    batch_id: str | None = None,
    timeout_sec: int = 1800,
) -> None:
    """Trigger one stage of olist-lakehouse/olist_pipeline.py inside the Spark master container."""
    if stage not in VALID_SPARK_STAGES:
        raise ValueError(f"Unsupported Spark stage: {stage}")
    container = Variable.get("LAKEHOUSE_SPARK_CONTAINER", default_var="olist-spark-master")
    uri = source_uri or Variable.get("LAKEHOUSE_SOURCE_URI", default_var=f"s3a://olist-lake/{BUCKET_PREFIX}")
    cmd = [
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        "/opt/spark/olist_pipeline.py",
        "--stage", stage,
        "--layers-dir", "/opt/spark/layers",
        "--mode", mode,
        "--source-uri", uri,
    ]
    if batch_id:
        cmd.extend(["--batch-id", batch_id])
    _exec_in_container(container, cmd, timeout_sec=timeout_sec)


def run_clickhouse_branch(branch: str, timeout_sec: int = 600) -> None:
    """Recreate __new staging tables, load Parquet exports, and atomically EXCHANGE into serving tables."""
    if branch not in VALID_CH_BRANCHES:
        raise ValueError(f"Unsupported ClickHouse serving branch: {branch}")
    container = Variable.get("LAKEHOUSE_CLICKHOUSE_CONTAINER", default_var="olist-clickhouse")
    script = (
        f"set -e && "
        f"clickhouse-client --multiquery < /opt/olist/clickhouse/{branch}/init_{branch}.sql && "
        f"clickhouse-client --multiquery < /opt/olist/clickhouse/{branch}/load_{branch}.sql"
    )
    _exec_in_container(container, ["bash", "-c", script], timeout_sec=timeout_sec)


def latest_landing_manifest() -> str | None:
    """Return the newest committed manifest key with ``total_rows > 0`` so idle 0-row ticks keep ShortCircuit effective."""
    keys = sorted(
        (k for k in list_keys(f"{BUCKET_PREFIX}/_manifests/") if k.endswith("manifest.json")),
        reverse=True,
    )
    s3 = s3_client()
    bkt = bucket()
    for key in keys:
        payload = json.loads(s3.get_object(Bucket=bkt, Key=key)["Body"].read())
        if int(payload.get("total_rows") or 0) > 0:
            return key
    return None
