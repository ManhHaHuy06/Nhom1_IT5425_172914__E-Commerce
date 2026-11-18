"""Client for the Integration-owned Spark runtime (not the Lakehouse Spark containers)."""
from typing import List, Optional

import requests
from airflow.models import Variable


def run_spark(stage: str, source_uri: Optional[str] = None, batch_id: Optional[str] = None, skip_tables: Optional[List[str]] = None):
    url = Variable.get("SPARK_RUNNER_URL", default_var="http://spark-job-runner:8090").rstrip("/") + "/run"
    token = Variable.get("SPARK_RUNNER_TOKEN")
    payload = {"stage": stage, "source_uri": source_uri, "batch_id": batch_id, "skip_tables": skip_tables or []}
    response = requests.post(url, json=payload, headers={"X-Runner-Token": token}, timeout=(15, 21600))
    if response.status_code >= 400:
        raise RuntimeError(f"Spark runner failed ({response.status_code}): {response.text[-12000:]}")
    body = response.json()
    if body.get("status") != "success":
        raise RuntimeError(f"Unexpected Spark runner response: {body}")
    return body
