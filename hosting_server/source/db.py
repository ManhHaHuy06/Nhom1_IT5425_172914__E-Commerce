"""Shared DB helpers for the simulator (connects as the sim_rw user)."""
import os

import numpy as np
import pandas as pd
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


def py(v):
    """numpy / pandas scalar -> plain python value pymysql can encode (NaN/NaT/NA -> None)."""
    if v is None or (not isinstance(v, (str, bytes)) and pd.isna(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if isinstance(v, np.generic):
        return v.item()
    return v


def to_rows(df, cols):
    return [tuple(py(v) for v in r) for r in df[cols].astype(object).itertuples(index=False, name=None)]


def insert_ignore(cur, table, df, cols=None, chunk=None):
    """INSERT IGNORE the columns ``cols`` (default: all) of ``df``; idempotent on the table's keys."""
    cols = list(cols or df.columns)
    data = to_rows(df, cols)
    sql = f"INSERT IGNORE INTO {table} ({','.join(cols)}) VALUES ({','.join(['%s'] * len(cols))})"
    step = chunk or len(data) or 1
    for i in range(0, len(data), step):
        cur.executemany(sql, data[i:i + step])
    return len(data)
