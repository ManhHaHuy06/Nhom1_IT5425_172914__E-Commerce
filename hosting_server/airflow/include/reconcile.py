"""Landing zone completeness check for the watermark tables (used by DAG reconcile_landing).

Invariant: every current OLTP row whose ``updated_at`` is at or below the committed watermark ``wm__<table>`` has
exactly that version (primary key + ``updated_at``) somewhere in ``landing/<table>/``.

Why it can break: ``updated_at`` is set when the statement runs, not when the transaction commits. A transaction that
commits more than ``LAG_SECONDS`` after its writes makes rows visible behind a window the extract already read, and
the next window starts after them, so they are never landed. Rows updated again later are not affected (their new
version is above the watermark and will be read by a future window).
"""
import pandas as pd
from sqlalchemy import text

from landing import BUCKET_PREFIX
from oltp import engine
from s3_utils import list_keys, read_parquet

# Primary key of each watermark table (order_reviews: surrogate review_pk, review_id is not unique)
KEYS = {
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_pk"],
}


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    # OLTP updated_at is TIMESTAMP(3); Parquet stores microseconds -> compare at millisecond precision
    return df.assign(updated_at=pd.to_datetime(df["updated_at"]).dt.floor("ms"))


def oltp_versions(table: str, watermark: str) -> pd.DataFrame:
    cols = KEYS[table] + ["updated_at"]
    with engine().connect() as conn:
        df = pd.read_sql(text(f"SELECT {', '.join(cols)} FROM {table} WHERE updated_at <= :wm"),
                         conn, params={"wm": watermark})
    return _normalize(df)


def landing_versions(table: str) -> tuple[pd.DataFrame, int]:
    """(distinct landed versions of ``table``, number of Parquet objects read)."""
    cols = KEYS[table] + ["updated_at"]
    keys = [k for k in list_keys(f"{BUCKET_PREFIX}/{table}/") if k.endswith(".parquet")]
    frames = [read_parquet(k, columns=cols).to_pandas() for k in keys]
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=cols)
    return _normalize(df).drop_duplicates(), len(keys)


def check_table(table: str, watermark: str) -> dict:
    """Compare OLTP and Landing; ``missing`` holds OLTP versions (<= watermark) that are absent from Landing."""
    oltp = oltp_versions(table, watermark)
    landed, objects = landing_versions(table)
    on = KEYS[table] + ["updated_at"]
    merged = oltp.merge(landed, on=on, how="left", indicator=True)
    missing = merged.loc[merged["_merge"] == "left_only", on].sort_values("updated_at")
    return {"table": table, "watermark": watermark, "oltp_rows": len(oltp), "landing_versions": len(landed),
            "landing_objects": objects, "missing": missing}
