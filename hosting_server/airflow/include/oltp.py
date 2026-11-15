"""OLTP (source) access for Airflow tasks. Credentials come from Airflow Variables (set from .env)."""
from functools import lru_cache

from airflow.models import Variable
from sqlalchemy import create_engine


@lru_cache(maxsize=1)
def engine():
    """One pooled engine per task process."""
    return create_engine(Variable.get("OLTP_URL"), pool_pre_ping=True)
