"""Phiên Trino dùng chung của notebook: chạy SQL, đọc kết quả ra DataFrame, tạo bảng nhỏ từ DataFrame.

Kết nối hiện tại nằm ở `db.con` (do `tables.connect` gán); module khác truy cập qua `db.con`, không `from db import con`.
"""
import numpy as np
import pandas as pd


def split_statements(sql: str) -> list[str]:
    """Tách chuỗi SQL thành từng câu theo `;` (bỏ comment `--`, không tách trong '...' hay "...").

    Trino chỉ nhận 1 câu / request và không nhận `;` ở cuối câu.
    """
    statements, buf, i, quote = [], [], 0, None
    while i < len(sql):
        ch = sql[i]
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
        elif ch in ("'", '"'):
            quote = ch
            buf.append(ch)
        elif sql.startswith("--", i):
            while i < len(sql) and sql[i] != "\n":
                i += 1
            continue
        elif ch == ";":
            statements.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    statements.append("".join(buf))
    return [s.strip() for s in statements if s.strip()]


class TrinoSession:
    """Kết nối Trino cho notebook: `execute` chạy được nhiều câu cách nhau `;`, `query` trả về (dòng, mô tả cột)."""

    def __init__(self, conn):
        self.conn = conn

    def query(self, sql: str):
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            return rows, cur.description
        finally:
            cur.close()

    def execute(self, sql: str) -> "TrinoSession":
        for statement in split_statements(sql):
            self.query(statement)
        return self

    def close(self):
        self.conn.close()


con: TrinoSession | None = None

_NUMERIC_TYPES = ("tinyint", "smallint", "integer", "bigint", "real", "double", "decimal")


def _sql_literal(value) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "NULL"
    if isinstance(value, (bool, np.bool_)):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, np.integer, np.floating)):
        return f"CAST({value!r} AS DOUBLE)" if isinstance(value, (float, np.floating)) else str(int(value))
    return "'" + str(value).replace("'", "''") + "'"


def create_table_from_df(name: str, df: pd.DataFrame):
    """Tạo bảng nhỏ trong EDA_SCHEMA từ DataFrame (thay cho `register` của DuckDB): CREATE TABLE AS VALUES."""
    rows = ", ".join("(" + ", ".join(_sql_literal(v) for v in row) + ")" for row in df.itertuples(index=False))
    con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM (VALUES {rows}) AS t({', '.join(df.columns)})")


def q(sql: str) -> pd.DataFrame:
    """Chạy 1 câu SELECT trên Trino, trả về DataFrame (cột số → kiểu số, cột DATE/TIMESTAMP → datetime64)."""
    rows, description = con.query(sql)
    df = pd.DataFrame(rows, columns=[c[0] for c in description])
    for name, type_code, *_ in description:
        type_code = str(type_code).lower()
        if type_code.startswith(_NUMERIC_TYPES):
            df[name] = pd.to_numeric(df[name])
        elif type_code.startswith(("date", "timestamp")):
            df[name] = pd.to_datetime(df[name])
    return df


def scalar(sql: str):
    """Giá trị đầu tiên của câu SELECT (giữ kiểu Python: date, list…)."""
    rows, _ = con.query(sql)
    return rows[0][0]
