"""Phân khúc RFM (1 dòng / khách, trên đơn hợp lệ) — giống `dim_customer_profile` của Gold."""
from typing import NamedTuple

from . import db
from .config import GOLD
from .db import scalar
from .sql import quantile_sql

# Quy tắc lấy từ Gold (olist-lakehouse/layers/gold/config.py), để phân khúc trong EDA = phân khúc trên dashboard.
RFM_QUINTILES = GOLD.RFM_QUANTILES       # R, M chấm theo ngũ phân vị
RFM_F_CUT = GOLD.RFM_FREQUENCY_CUTS      # F: ngưỡng cố định 1 | 2 | 3 | 4 | 5+ đơn (≈97% khách chỉ mua 1 đơn)
# Xét theo thứ tự: khách thuộc quy tắc đầu tiên khớp (Champions → Loyal → Khách mới → Cần nuôi dưỡng → Giá trị cao
# sắp mất); khách không khớp quy tắc nào là RFM_OTHER (F = 1, R <= 2, M <= 3)
RFM_SEGMENT_RULES = GOLD.RFM_SEGMENTS
RFM_OTHER = GOLD.RFM_OTHER_SEGMENT
SEG_ORDER = [name for name, _ in RFM_SEGMENT_RULES] + [RFM_OTHER]


class Rfm(NamedTuple):
    snapshot: object   # ngày chốt số liệu = ngày mua cuối + 1
    r_cut: list        # 4 ngưỡng số ngày (ngũ phân vị)
    m_cut: list        # 4 ngưỡng tổng GMV (ngũ phân vị)


def _score_sql(col: str, cuts, lower_is_better: bool = False) -> str:
    # Chấm 1–5 theo 4 ngưỡng; lower_is_better (recency) thì đảo điểm
    scores = [5, 4, 3, 2] if lower_is_better else [1, 2, 3, 4]
    whens = " ".join(f"WHEN {col} <= {c} THEN {s}" for c, s in zip(cuts, scores))
    return f"CASE {whens} ELSE {1 if lower_is_better else 5} END"


def create_customer_rfm() -> Rfm:
    """`customer_rfm`: 1 dòng / khách (`customer_key` = 1 `customer_unique_id`) với recency_days, frequency, monetary,
    điểm R/F/M 1–5, `rfm_code` và `segment` (quy tắc ở RFM_SEGMENT_RULES)."""
    snapshot = scalar("SELECT max(purchase_date) + INTERVAL '1' DAY FROM order_base WHERE is_valid = 1")
    db.con.execute(f"""
    CREATE OR REPLACE TABLE customer_rfm_base AS
    SELECT customer_key,
           date_diff('day', max(purchase_date), DATE '{snapshot}') AS recency_days,
           count(*) AS frequency, sum(gmv) AS monetary
    FROM order_base WHERE is_valid = 1 GROUP BY 1
    """)
    r_cut = scalar(f"SELECT {quantile_sql('recency_days', RFM_QUINTILES)} FROM customer_rfm_base")
    m_cut = scalar(f"SELECT {quantile_sql('monetary', RFM_QUINTILES)} FROM customer_rfm_base")
    segment = "CASE " + " ".join(f"WHEN {cond} THEN '{name}'" for name, cond in RFM_SEGMENT_RULES) + f" ELSE '{RFM_OTHER}' END"
    db.con.execute(f"""
    CREATE OR REPLACE TABLE customer_rfm AS
    WITH s AS (
        SELECT *, {_score_sql("recency_days", r_cut, lower_is_better=True)} AS R,
                  {_score_sql("frequency", RFM_F_CUT)} AS F,
                  {_score_sql("monetary", m_cut)} AS M
        FROM customer_rfm_base)
    SELECT *, CAST(R AS VARCHAR) || CAST(F AS VARCHAR) || CAST(M AS VARCHAR) AS rfm_code,   -- vd. '545'
           {segment} AS segment
    FROM s
    """)
    return Rfm(snapshot, r_cut, m_cut)
