"""Build Gold marts for the Power BI dashboard from Silver, then export Parquet for ClickHouse.

Thiết kế: docs/GOLD_DESIGN.md. Logic khớp olist-eda/powerbi/build_data*.py và build_repurchase.py
(phiên bản CSV cũ) để dashboard đổi nguồn mà không đổi ý nghĩa số liệu.

Cấu trúc:
- `compute_marts()` chỉ biến đổi DataFrame (không đọc/ghi bảng) → test được bằng Spark local.
- `build_marts()` đọc Silver, ghi từng bảng Gold (ghi đè toàn bộ), rồi export Parquet.
"""

from __future__ import annotations

import calendar
from datetime import date, datetime
from typing import Callable

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

from layers.gold import config as C

SILVER = "iceberg.silver"
GOLD = "iceberg.gold"
EXPORT_ROOT = "s3a://lakehouse/exports/gold"

SILVER_TABLES = (
    "fact_orders", "fact_order_items", "fact_payments", "fact_reviews",
    "dim_customer", "dim_seller", "dim_product", "dim_category",
)
# Thứ tự build = thứ tự phụ thuộc.
GOLD_TABLES = (
    "fact_order_summary", "fact_item_sales", "report_period",
    "dim_date", "dim_state", "dim_category", "dim_seller", "dim_customer_profile",
)

Materialize = Callable[[str, DataFrame], DataFrame]


# =============================================================================== helpers
def conform(df: DataFrame, schema: StructType) -> DataFrame:
    """Chọn và ép kiểu cột theo đúng schema bảng đích (DDL là nguồn sự thật)."""
    missing = [f.name for f in schema if f.name not in df.columns]
    if missing:
        raise ValueError(f"DataFrame thiếu cột so với DDL: {missing}")
    return df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in schema])


def band(value: Column, bands: list, inclusive: bool = False) -> tuple[Column, Column]:
    """(nhãn, thứ tự) theo danh sách (cận trên, nhãn); cận trên None = phần còn lại. Giá trị null → null."""
    label = order = None
    for i, (cut, name) in enumerate(bands, 1):
        if cut is None:
            label, order = label.otherwise(F.lit(name)), order.otherwise(F.lit(i))
            break
        cond = value <= cut if inclusive else value < cut
        label = F.when(cond, F.lit(name)) if label is None else label.when(cond, F.lit(name))
        order = F.when(cond, F.lit(i)) if order is None else order.when(cond, F.lit(i))
    return (F.when(value.isNull(), F.lit(None)).otherwise(label),
            F.when(value.isNull(), F.lit(None)).otherwise(order))


def flag(cond: Column) -> Column:
    return cond.cast("int")


def yes_no(cond: Column) -> Column:
    return F.when(cond, F.lit("Có")).otherwise(F.lit("Không"))


def region_map(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(C.REGION_ROWS, "state STRING, region STRING, region_order INT")


def median(df: DataFrame, column: str) -> float | None:
    return df.agg(F.expr(f"percentile({column}, 0.5)").alias("m")).first()["m"]


# =============================================================================== fact_order_summary
def item_categories(products: DataFrame, categories: DataFrame, english_only: bool = False) -> DataFrame:
    cat = categories.select("category_key", "product_category_name_english")
    fallback = [] if english_only else ["product_category_name"]
    return products.join(cat, "category_key", "left").select(
        "product_key", "product_id",
        F.coalesce("product_category_name_english", *fallback, F.lit("unknown")).alias("category"),
    )


def build_order_summary(spark: SparkSession, s: dict[str, DataFrame]) -> DataFrame:
    # main_category: chỉ tên tiếng Anh, thiếu bản dịch = 'unknown' (khác `category` của item_sales,
    # giữ đúng như customer_behaviour.ipynb / order_summary.csv cũ)
    items = (s["fact_order_items"].join(item_categories(s["dim_product"], s["dim_category"], english_only=True),
                                        "product_key", "left")
             .withColumn("category", F.coalesce("category", F.lit("unknown"))))
    by_price = Window.partitionBy("order_key").orderBy(F.col("price").desc(), F.col("order_item_id"))
    item_order = (
        items.withColumn("_rn", F.row_number().over(by_price))
        .groupBy("order_key")
        .agg(
            F.count(F.lit(1)).alias("n_items"),
            F.countDistinct("product_key").alias("n_products"),
            F.countDistinct("seller_key").alias("n_sellers"),
            F.round(F.sum("price"), 2).alias("product_value"),
            F.round(F.sum("freight_value"), 2).alias("freight_value"),
            F.max(F.when(F.col("_rn") == 1, F.col("category"))).alias("main_category"),
        )
    )

    by_value = Window.partitionBy("order_key").orderBy(F.col("payment_value").desc(), F.col("payment_sequential"))
    pay_order = (
        s["fact_payments"].withColumn("_rn", F.row_number().over(by_value))
        .groupBy("order_key")
        .agg(
            F.max("payment_installments").alias("installments"),
            F.max(F.when(F.col("_rn") == 1, F.col("payment_type"))).alias("payment_type"),
        )
    )

    # Review → grain đơn: điểm TB; không hài lòng nếu có ≥1 review 1–2★; hài lòng nếu mọi review 4–5★
    rev_order = (
        s["fact_reviews"].filter(F.col("review_score").between(1, 5))
        .groupBy("order_key")
        .agg(
            F.avg("review_score").alias("review_score"),
            F.max(flag(F.col("review_score") <= 2)).alias("is_negative"),
            F.min(flag(F.col("review_score") >= 4)).alias("is_positive"),
        )
    )

    customers = s["dim_customer"].select(
        "customer_key", "state",
        F.when(F.col("state").isNotNull() & F.col("city").isNotNull(),
               F.concat_ws("|", "state", "city")).alias("customer_city_key"),
    )

    purchase_date = F.to_date("order_purchase_timestamp")
    delivered = F.to_date("order_delivered_customer_date")
    estimated = F.to_date("order_estimated_delivery_date")
    os_ = (
        s["fact_orders"]
        .select(
            "order_key", "order_id", "customer_key", "order_status",
            purchase_date.alias("purchase_date"),
            F.hour("order_purchase_timestamp").alias("purchase_hour"),
            flag(F.col("order_status") == "delivered").alias("is_delivered"),
            # số ngày theo ngày lịch (giống notebook / CSV cũ), không theo hiệu timestamp như Silver
            F.datediff(delivered, purchase_date).alias("delivery_days"),
            F.datediff(estimated, purchase_date).alias("estimated_delivery_days"),
            F.datediff(delivered, estimated).alias("delivery_delay_days"),
        )
        .join(customers, "customer_key", "left")
        .join(region_map(spark), "state", "left")
        .join(item_order, "order_key", "left")
        .join(pay_order, "order_key", "left")
        .join(rev_order, "order_key", "left")
        .withColumn("payment_type", F.coalesce("payment_type", F.lit("unknown")))
        .withColumn("order_gmv", F.round(F.col("product_value") + F.col("freight_value"), 2))
        .withColumn("is_valid", flag(~F.col("order_status").isin(*C.INVALID_ORDER_STATUSES)
                                     & F.col("n_items").isNotNull()))
    )

    value_band, value_order = band(F.col("order_gmv"), C.ORDER_VALUE_BANDS)
    inst = F.least(F.greatest(F.col("installments"), F.lit(1)), F.lit(C.MAX_INSTALLMENT_BAND))
    is_card = F.col("payment_type") == "credit_card"
    has_delivery = ((F.col("is_delivered") == 1) & F.col("delivery_delay_days").isNotNull()
                    & F.col("delivery_days").isNotNull())
    delay_label, delay_order = band(F.col("delivery_delay_days"), C.DELAY_BANDS, inclusive=True)
    score_int = F.round("review_score").cast("int")

    return (
        os_.withColumn("order_value_band", value_band)
        .withColumn("band_order", value_order)
        .withColumn("inst_order", F.when(is_card, inst))
        .withColumn("installment_band", F.when(
            is_card, F.when(inst == C.MAX_INSTALLMENT_BAND, F.lit(f"{C.MAX_INSTALLMENT_BAND}+"))
            .otherwise(inst.cast("string"))))
        .withColumn("is_multi_product", flag(F.col("n_products") > 1))
        .withColumn("is_late", F.when(has_delivery, flag(F.col("delivery_delay_days") > 0)))
        .withColumn("score_int", score_int)
        .withColumn("score_label", F.when(score_int.isNotNull(), F.concat(score_int.cast("string"), F.lit("★"))))
        .withColumn("delay_bucket", F.when(has_delivery, delay_label))
        .withColumn("delay_bucket_order", F.when(has_delivery, delay_order))
        .withColumn("delivery_status", F.when(has_delivery, F.when(F.col("delivery_delay_days") > 0, "Trễ hạn")
                                              .otherwise("Đúng hạn")))
        .withColumn("delivery_days_capped", F.when(has_delivery, F.least("delivery_days", F.lit(40))))
        .withColumn("items_band", F.when(F.col("n_items") >= 5, F.lit("5+")).otherwise(F.col("n_items").cast("string")))
        .withColumn("items_band_order", F.least("n_items", F.lit(5)))
        .withColumn("sellers_band", F.when(F.col("n_sellers") >= 2, F.lit("≥2 người bán"))
                    .when(F.col("n_sellers") == 1, F.lit("1 người bán")))
        .withColumn("sellers_band_order", F.least("n_sellers", F.lit(2)))
    )


# =============================================================================== fact_item_sales
def build_item_sales(spark: SparkSession, s: dict[str, DataFrame], orders: DataFrame) -> DataFrame:
    valid = orders.filter(F.col("is_valid") == 1).select(
        "order_key", "order_id", "customer_key", "purchase_date", F.col("state").alias("customer_state"))
    sellers = s["dim_seller"].select(
        "seller_key", "seller_id", F.col("state").alias("seller_state"),
        F.when(F.col("state").isNotNull() & F.col("city").isNotNull(),
               F.concat_ws("|", "state", "city")).alias("seller_city_key"))
    seller_regions = region_map(spark).select(
        F.col("state").alias("seller_state"), F.col("region").alias("seller_region"),
        F.col("region_order").alias("seller_region_order"))

    items = (
        s["fact_order_items"].select("order_key", "order_item_id", "product_key", "seller_key", "price", "freight_value")
        .join(valid, "order_key", "inner")
        .join(item_categories(s["dim_product"], s["dim_category"]), "product_key", "left")
        .join(sellers, "seller_key", "left")
        .join(seller_regions, "seller_state", "left")
        .withColumn("category", F.coalesce("category", F.lit("unknown")))
        .withColumn("item_gmv", F.round(F.col("price") + F.col("freight_value"), 2))
        .withColumn("units", F.count(F.lit(1)).over(Window.partitionBy("product_key")))
        .withColumn("is_local_seller", flag(F.col("seller_state") == F.col("customer_state")))
    )
    top = [r["category"] for r in items.groupBy("category").agg(F.sum("item_gmv").alias("g"))
           .orderBy(F.col("g").desc(), "category").limit(C.TOP_FLOW_CATEGORY_N).collect()]
    price_label, price_order = band(F.col("price"), C.PRICE_BANDS)
    sku_label, sku_order = band(F.col("units"), C.SKU_UNIT_BANDS, inclusive=True)
    return (
        items.withColumn("price_band", price_label).withColumn("price_band_order", price_order)
        .withColumn("sku_band", sku_label).withColumn("sku_band_order", sku_order)
        .withColumn("is_top12_category", yes_no(F.col("category").isin(top)))
    )


# =============================================================================== report_period
def build_report_period(orders: DataFrame, items: DataFrame) -> tuple[dict, DataFrame, DataFrame]:
    """Kỳ báo cáo + thống kê ngành / bang dùng chung cho dim_category, dim_state.

    Trả về (period, category_stats, state_stats)."""
    valid = orders.filter(F.col("is_valid") == 1)
    monthly = (valid.groupBy((F.year("purchase_date") * 100 + F.month("purchase_date")).alias("k"))
               .count().collect())
    if not monthly:
        raise RuntimeError("fact_order_summary không có đơn hợp lệ; không xác định được kỳ báo cáo")
    max_n = max(r["count"] for r in monthly)
    report_key = max(r["k"] for r in monthly if r["count"] >= C.REPORT_MONTH_MIN_SHARE * max_n)
    ry, rm = divmod(report_key, 100)
    pyr = ry - 1
    period = {
        "report_year": ry, "report_month": rm,
        "cy_start": date(ry, 1, 1), "cy_end": date(ry, rm, calendar.monthrange(ry, rm)[1]),
        "py_start": date(pyr, 1, 1), "py_end": date(pyr, rm, calendar.monthrange(pyr, rm)[1]),
        "trend_start": date(pyr, 1, 1), "report_month_start": date(ry, rm, 1),
        "label_cy": f"01–{rm:02d}/{ry}" if rm > 1 else f"01/{ry}",
        "label_py": f"01–{rm:02d}/{pyr}" if rm > 1 else f"01/{pyr}",
    }

    y, m = F.year("purchase_date"), F.month("purchase_date")
    is_cy, is_py = (y == ry) & (m <= rm), (y == pyr) & (m <= rm)

    # ---- ngành (product §3)
    cat = items.groupBy("category").agg(
        F.sum(F.when(F.col("purchase_date").between(F.lit(period["trend_start"]), F.lit(period["cy_end"])),
                     F.col("item_gmv"))).alias("gmv_window"),
        F.sum(F.when(is_py, F.col("item_gmv"))).alias("gmv_py"),
        F.sum(F.when(is_cy, F.col("item_gmv"))).alias("gmv_cy"),
    )
    totals = cat.agg(F.sum("gmv_cy").alias("cy"), F.sum("gmv_py").alias("py")).first()
    cat = (
        cat.withColumn("share_cy", F.col("gmv_cy") / F.lit(totals["cy"]) if totals["cy"] else F.lit(None))
        .withColumn("growth", F.when(F.col("gmv_py") > 0, F.col("gmv_cy") / F.col("gmv_py") - 1))
        .withColumn("window_rank", F.row_number().over(
            Window.orderBy(F.col("gmv_window").desc_nulls_last(), F.col("category"))))
        .withColumn("in_matrix", F.coalesce((F.col("share_cy") >= C.MIN_CATEGORY_SHARE) & (F.col("gmv_py") > 0),
                                            F.lit(False)))
    ).cache()
    period["market_growth"] = totals["cy"] / totals["py"] - 1 if totals["py"] else None
    period["category_share_median"] = median(cat.filter("in_matrix"), "share_cy")

    # ---- bang (geo §11)
    st = valid.filter(is_cy | is_py).groupBy("state").agg(
        F.sum(F.when(is_py, F.col("order_gmv"))).alias("rev_py"),
        F.sum(F.when(is_cy, F.col("order_gmv"))).alias("rev_cy"),
        F.sum(flag(is_py)).alias("ord_py"),
        F.sum(flag(is_cy)).alias("ord_cy"),
    )
    pop = orders.sparkSession.createDataFrame(C.state_population(), "state STRING, population_2018 BIGINT")
    st = (
        region_map(orders.sparkSession).join(pop, "state", "left").join(st, "state", "left")
        .withColumn("growth", F.when(F.col("rev_py") > 0, F.col("rev_cy") / F.col("rev_py") - 1))
        .withColumn("penetration", F.when(F.col("population_2018") > 0,
                                          F.col("ord_cy") / (F.col("population_2018") / 1e5)))
        .withColumn("in_matrix", F.coalesce(F.col("ord_py") >= C.STATE_MATRIX_MIN_PY_ORDERS, F.lit(False)))
    ).cache()
    period["state_penetration_median"] = median(st.filter("in_matrix"), "penetration")
    period["state_growth_median"] = median(st.filter("in_matrix"), "growth")
    return period, cat, st


def report_period_frame(spark: SparkSession, period: dict) -> DataFrame:
    cols = ["report_year", "report_month", "cy_start", "cy_end", "py_start", "py_end", "trend_start",
            "label_cy", "label_py", "market_growth", "category_share_median",
            "state_penetration_median", "state_growth_median"]
    row = [period[c] for c in cols] + [datetime.utcnow()]
    schema = ("report_year INT, report_month INT, cy_start DATE, cy_end DATE, py_start DATE, py_end DATE, "
              "trend_start DATE, label_cy STRING, label_py STRING, market_growth DOUBLE, "
              "category_share_median DOUBLE, state_penetration_median DOUBLE, state_growth_median DOUBLE, "
              "built_at TIMESTAMP")
    return spark.createDataFrame([row], schema)


# =============================================================================== dims
def build_dim_date(orders: DataFrame) -> DataFrame:
    iso_dow = (F.dayofweek("date") + 5) % 7 + 1  # Spark: 1 = CN → ISO: 1 = T2 … 7 = CN
    weekday = F.element_at(F.array(*[F.lit(d) for d in ["T2", "T3", "T4", "T5", "T6", "T7", "CN"]]), iso_dow)
    return (
        orders.agg(F.min("purchase_date").alias("d0"), F.max("purchase_date").alias("d1"))
        .select(F.explode(F.sequence("d0", "d1", F.expr("INTERVAL 1 DAY"))).alias("date"))
        .select(
            "date", F.year("date").alias("year"), F.month("date").alias("month"),
            F.date_format("date", "yyyy-MM").alias("year_month"),
            (F.year("date") * 100 + F.month("date")).alias("year_month_order"),
            iso_dow.alias("dow"), weekday.alias("weekday"),
        )
    )


def build_dim_state(st: DataFrame, period: dict) -> DataFrame:
    mx, my = period["state_penetration_median"], period["state_growth_median"]
    if mx is None or my is None:
        quadrant, order = F.lit("Ngoài ma trận"), F.lit(5)
    else:
        low_pen, grow = F.col("penetration") < mx, F.col("growth") >= my
        quadrant = (F.when(~F.col("in_matrix"), "Ngoài ma trận (<100 đơn kỳ gốc)")
                    .when(low_pen & grow, "Cơ hội").when(~low_pen & grow, "Ngôi sao")
                    .when(~low_pen, "Duy trì").otherwise("Cần xem xét"))
        order = (F.when(~F.col("in_matrix"), 5).when(low_pen & grow, 2).when(~low_pen & grow, 1)
                 .when(~low_pen, 3).otherwise(4))
    return st.select("state", "region", "region_order", "population_2018",
                     yes_no(F.col("in_matrix")).alias("in_matrix"),
                     quadrant.alias("quadrant"), order.alias("quadrant_order"))


def build_dim_category(cat: DataFrame, period: dict) -> DataFrame:
    med, mkt = period["category_share_median"], period["market_growth"]
    if med is None or mkt is None:
        quadrant, order = F.lit("Ngoài ma trận"), F.lit(5)
    else:
        big, grow = F.col("share_cy") >= med, F.col("growth") >= mkt
        quadrant = (F.when(~F.col("in_matrix"), "Ngoài ma trận").when(big & grow, "Ngôi sao")
                    .when(big, "Trụ cột chững lại").when(grow, "Đang lên").otherwise("Tụt lại"))
        order = F.when(~F.col("in_matrix"), 5).when(big & grow, 1).when(big, 2).when(grow, 3).otherwise(4)
    top = F.col("window_rank") <= C.TOP_CATEGORY_N
    return cat.select(
        "category",
        F.when(top, F.col("category")).otherwise(F.lit("Khác")).alias("category_group"),
        F.when(top, F.col("window_rank")).otherwise(F.lit(C.TOP_CATEGORY_N + 1)).alias("category_group_order"),
        yes_no(F.col("in_matrix")).alias("in_matrix"),
        quadrant.alias("quadrant"), order.alias("quadrant_order"),
    )


def build_dim_seller(s: dict[str, DataFrame], orders: DataFrame, items: DataFrame, period: dict) -> DataFrame:
    month = F.trunc("purchase_date", "month")
    base = items.groupBy("seller_key").agg(
        F.first("seller_id", ignorenulls=True).alias("seller_id"),
        F.first("seller_state", ignorenulls=True).alias("seller_state"),
        F.first("seller_city_key", ignorenulls=True).alias("seller_city_key"),
        F.first("seller_region", ignorenulls=True).alias("seller_region"),
        F.first("seller_region_order", ignorenulls=True).alias("seller_region_order"),
        F.sum("item_gmv").alias("gmv_exact"),
        F.countDistinct("order_key").alias("orders"),
        F.min(month).alias("first_month"),
        F.countDistinct("category").alias("n_categories"),
    )
    quality = (
        items.select("seller_key", "order_key").distinct()
        .join(orders.select("order_key", "review_score", "is_negative", "is_late"), "order_key")
        .groupBy("seller_key")
        .agg(F.round(F.avg("review_score"), 4).alias("avg_review"),
             F.round(F.avg("is_negative"), 4).alias("negative_rate"),
             F.round(F.avg("is_late"), 4).alias("late_rate"))
    )
    active = (
        items.select("seller_key", month.alias("month")).distinct()
        .join(base.select("seller_key", "first_month"), "seller_key")
        .groupBy("seller_key")
        .agg(F.max(flag(F.col("month").between(F.add_months("first_month", C.ACTIVE_FROM_MONTH),
                                                F.add_months("first_month", C.ACTIVE_TO_MONTH)))).alias("active_m6_8"))
    )
    tier, tier_order = band(F.col("gmv_exact"), C.SELLER_TIERS)
    rev, rev_order = band(F.col("avg_review"), C.SELLER_REVIEW_BANDS)
    few = (F.col("orders") < C.SELLER_MIN_ORDERS) | F.col("avg_review").isNull()
    cat_band, cat_order = band(F.col("n_categories"), C.CATEGORY_COUNT_BANDS, inclusive=True)
    early = F.col("first_month") < F.lit(period["trend_start"])
    return (
        base.join(quality, "seller_key", "left").join(active, "seller_key", "left")
        .withColumn("gmv", F.round("gmv_exact", 2))
        # xếp hạng trên số đã làm tròn: 2 người bán bằng GMV tới từng xu là đồng hạng (sai số cộng số thực
        # theo thứ tự khác nhau không được làm đổi thứ hạng)
        .withColumn("gmv_rank", F.rank().over(Window.orderBy(F.col("gmv").desc())))
        .withColumn("seller_tier", tier).withColumn("seller_tier_order", tier_order)
        .withColumn("first_year", F.year("first_month"))
        .withColumn("seller_cohort", F.when(early, F.year("first_month").cast("string"))
                    .otherwise(F.concat(F.year("first_month").cast("string"), F.lit("-Q"),
                                        F.quarter("first_month").cast("string"))))
        .withColumn("seller_cohort_order", F.when(early, F.year("first_month") * 100)
                    .otherwise(F.year("first_month") * 100 + F.quarter("first_month")))
        .withColumn("retention_observable", yes_no(
            F.add_months("first_month", C.ACTIVE_TO_MONTH) <= F.lit(period["report_month_start"])))
        .withColumn("review_band", F.when(few, F.lit(C.SELLER_REVIEW_BAND_FEW_ORDERS)).otherwise(rev))
        .withColumn("review_band_order", F.when(few, F.lit(len(C.SELLER_REVIEW_BANDS) + 1)).otherwise(rev_order))
        .withColumn("category_band", cat_band).withColumn("category_band_order", cat_order)
    )


def build_customer_profile(s: dict[str, DataFrame], orders: DataFrame) -> DataFrame:
    valid = orders.filter(F.col("is_valid") == 1)

    # ---- RFM: toàn kỳ, đơn hợp lệ; R, M theo ngũ phân vị, F theo ngưỡng cố định
    ref_date = valid.agg(F.date_add(F.max("purchase_date"), 1).alias("d")).first()["d"]
    rfm = valid.groupBy("customer_key").agg(
        F.datediff(F.lit(ref_date), F.max("purchase_date")).alias("recency_days"),
        F.count(F.lit(1)).alias("frequency"),
        F.round(F.sum("order_gmv"), 2).alias("monetary_raw"),
        F.sum("order_gmv").alias("monetary_exact"),
    )
    q = ", ".join(str(x) for x in C.RFM_QUANTILES)
    cuts = rfm.agg(F.expr(f"percentile(recency_days, array({q}))").alias("r"),
                   F.expr(f"percentile(monetary_exact, array({q}))").alias("m")).first()

    def score(col: str, edges, lower_is_better: bool = False) -> Column:
        scores = [5, 4, 3, 2] if lower_is_better else [1, 2, 3, 4]
        expr = None
        for edge, sc in zip(edges, scores):
            expr = (F.when(F.col(col) <= edge, sc) if expr is None else expr.when(F.col(col) <= edge, sc))
        return expr.otherwise(1 if lower_is_better else 5)

    segment = "CASE " + " ".join(f"WHEN {c} THEN '{n}'" for n, c in C.RFM_SEGMENTS) + \
              f" ELSE '{C.RFM_OTHER_SEGMENT}' END"
    segment_order = "CASE " + " ".join(f"WHEN {c} THEN {i}" for i, (_, c) in enumerate(C.RFM_SEGMENTS, 1)) + \
                    f" ELSE {len(C.RFM_SEGMENTS) + 1} END"
    rfm = (
        rfm.withColumn("R", score("recency_days", cuts["r"], lower_is_better=True))
        .withColumn("F", score("frequency", C.RFM_FREQUENCY_CUTS))
        .withColumn("M", score("monetary_exact", cuts["m"]))
        .withColumn("segment", F.expr(segment))
        .withColumn("segment_order", F.expr(segment_order))
        .select("customer_key", "recency_days", "frequency", F.col("monetary_raw").alias("monetary"),
                "R", "F", "M", "segment", "segment_order")
    )

    # ---- Khoảng cách tới đơn thứ 2 (đơn hợp lệ); 2 đơn cùng ngày đầu = 0 ngày
    first = valid.groupBy("customer_key").agg(F.min("purchase_date").alias("f"), F.count(F.lit(1)).alias("n"))
    gap = (
        valid.select("customer_key", "purchase_date").join(first.filter(F.col("n") >= 2), "customer_key")
        .groupBy("customer_key")
        .agg(F.sum(flag(F.col("purchase_date") == F.col("f"))).alias("same_day"),
             F.min(F.when(F.col("purchase_date") > F.col("f"), F.col("purchase_date"))).alias("next_date"),
             F.first("f").alias("f"))
        .select("customer_key", F.when(F.col("same_day") >= 2, F.lit(0))
                .otherwise(F.datediff("next_date", "f")).alias("days_to_2nd"))
    )
    gap_label, gap_order = band(F.col("days_to_2nd"), C.GAP_BANDS, inclusive=True)
    gap = gap.withColumn("gap_bucket", gap_label).withColumn("gap_order", gap_order)

    # ---- Mua lại trong 90 ngày theo điểm đơn đầu (MỌI đơn, như customer_experience.ipynb)
    ts = s["fact_orders"].select("order_key", "customer_key", F.col("order_purchase_timestamp").alias("ts"))
    ts = ts.join(orders.select("order_key", "score_int"), "order_key", "left")
    max_ts = ts.agg(F.max("ts").alias("t")).first()["t"]
    w = Window.partitionBy("customer_key").orderBy("ts", "order_key")
    window = F.expr(f"INTERVAL {C.REPURCHASE_WINDOW_DAYS} DAYS")
    first_order = (
        ts.withColumn("_rn", F.row_number().over(w)).withColumn("next_ts", F.lead("ts").over(w))
        .filter(F.col("_rn") == 1)
        .select(
            "customer_key",
            F.to_date("ts").alias("first_purchase_date"),
            F.col("score_int").alias("first_score_int"),
            F.when(F.col("score_int").isNotNull(), F.concat(F.col("score_int").cast("string"), F.lit("★")))
            .alias("first_score_label"),
            flag(F.col("next_ts").isNotNull() & (F.col("next_ts") <= F.col("ts") + window)).alias("repurchase_90d"),
            flag(F.col("ts") <= F.lit(max_ts) - window).alias("repurchase_observable"),
        )
    )

    customers = s["dim_customer"].select("customer_key", "customer_unique_id", "state")
    return (
        orders.select("customer_key").distinct()
        .join(customers, "customer_key", "left")
        .join(region_map(orders.sparkSession).select("state", "region"), "state", "left")
        .join(rfm, "customer_key", "left")
        .join(gap, "customer_key", "left")
        .join(first_order, "customer_key", "left")
    )


# =============================================================================== orchestration
def compute_marts(spark: SparkSession, silver: dict[str, DataFrame], materialize: Materialize) -> dict[str, DataFrame]:
    """Tính mọi bảng Gold theo thứ tự phụ thuộc. `materialize(name, df)` ghi/cache rồi trả lại DataFrame để
    bước sau đọc từ kết quả đã ghi (cắt lineage)."""
    out: dict[str, DataFrame] = {}
    out["fact_order_summary"] = orders = materialize("fact_order_summary", build_order_summary(spark, silver))
    out["fact_item_sales"] = items = materialize("fact_item_sales", build_item_sales(spark, silver, orders))
    period, cat, st = build_report_period(orders, items)
    print("Kỳ báo cáo:", period["label_cy"], "so với", period["label_py"])
    out["report_period"] = materialize("report_period", report_period_frame(spark, period))
    out["dim_date"] = materialize("dim_date", build_dim_date(orders))
    out["dim_state"] = materialize("dim_state", build_dim_state(st, period))
    out["dim_category"] = materialize("dim_category", build_dim_category(cat, period))
    out["dim_seller"] = materialize("dim_seller", build_dim_seller(silver, orders, items, period))
    out["dim_customer_profile"] = materialize("dim_customer_profile", build_customer_profile(silver, orders))
    return out


def export_table(spark: SparkSession, table: str) -> None:
    path = f"{EXPORT_ROOT}/{table}"
    spark.table(f"{GOLD}.{table}").write.mode("overwrite").parquet(path)
    print(f"Exported {GOLD}.{table} -> {path}")


def build_marts(spark: SparkSession) -> None:
    silver = {name: spark.table(f"{SILVER}.{name}") for name in SILVER_TABLES}

    def materialize(name: str, df: DataFrame) -> DataFrame:
        table = f"{GOLD}.{name}"
        conformed = conform(df, spark.table(table).schema)
        # Ghi đè toàn bộ: cột "ảnh chụp theo kỳ" có thể đổi trên mọi dòng ở mỗi lần build.
        conformed.writeTo(table).overwrite(F.lit(True))
        count = spark.table(table).count()
        print(f"{table}: {count} rows")
        if count == 0:
            raise RuntimeError(f"{table} is empty")
        return spark.table(table)

    compute_marts(spark, silver, materialize)
    for name in GOLD_TABLES:
        export_table(spark, name)
