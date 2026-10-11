"""OLTP (source) access for Airflow tasks. Credentials come from Airflow Variables (set from .env)."""
from airflow.models import Variable
from sqlalchemy import create_engine


def engine():
    return create_engine(Variable.get("OLTP_URL"), pool_pre_ping=True)
