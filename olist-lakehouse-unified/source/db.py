"""Shared DB helpers for the simulator (connects as the sim_rw user)."""
import os

import pymysql


def connect(autocommit=False):
    return pymysql.connect(
        host=os.environ.get("OLTP_HOST", "localhost"),
        port=int(os.environ.get("OLTP_PORT", 3307)),
        user=os.environ["OLTP_SIM_USER"],
        password=os.environ["OLTP_SIM_PASSWORD"],
        database=os.environ["OLTP_DATABASE"],
        charset="utf8mb4",
        autocommit=autocommit,
    )
