"""Connection helpers for the simulated shop MySQL database."""
from airflow.models import Variable
from sqlalchemy import create_engine
from sqlalchemy.engine import URL


def engine():
    url = URL.create(
        "mysql+pymysql",
        username=Variable.get("OLTP_ETL_USER"),
        password=Variable.get("OLTP_ETL_PASSWORD"),
        host=Variable.get("OLTP_HOST", default_var="oltp-mysql"),
        port=int(Variable.get("OLTP_PORT", default_var="3306")),
        database=Variable.get("OLTP_DATABASE", default_var="shop"),
        query={"charset": "utf8mb4"},
    )
    return create_engine(url, pool_pre_ping=True, pool_recycle=1800, connect_args={"connect_timeout": 15})
