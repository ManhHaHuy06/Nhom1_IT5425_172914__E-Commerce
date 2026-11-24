import warnings
import os
from html import escape
from pathlib import Path

import clickhouse_connect
from dotenv import load_dotenv
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

warnings.filterwarnings("ignore")

st.set_page_config(
    page_title="E-commerce Intelligence",
    page_icon="📊",
    layout="wide"
)

# Streamlit fades the caption container, including explicitly black text.
st.html(
    '''<style>
    [data-testid="stCaptionContainer"]:has(.dashboard-caption-text) {
        opacity: 1;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] label:has(input[type="radio"]) p,
    [data-testid="stSidebar"] [data-testid="stDateInput"] [role="spinbutton"],
    [data-testid="stSidebar"] [data-testid="stSelectbox"] input[role="combobox"] {
        font-weight: 600;
    }
    .pielayer .dashboard-pie-leader-managed > path.textline {
        display: none;
    }
    </style>'''
)


# Add leaders where Plotly leaves outside pie labels without a connector.
st.html(
    r'''<script>
(() => {
    if (window.dashboardPieLeaders) return;
    window.dashboardPieLeaders = true;

    const leaderClass = "dashboard-pie-leader";
    let framePending = false;

    function drawLeaders() {
        framePending = false;
        for (const slice of document.querySelectorAll(".js-plotly-plot .pielayer .slice")) {
            let line = slice.querySelector("." + leaderClass);
            const label = slice.querySelector(".slicetext text");
            const point = slice.__data__;
            const matrix = slice.getScreenCTM();
            const hasSlice = point?.v > 0;
            if (!hasSlice || !label || !point.pxmid || !matrix) {
                slice.classList.toggle("dashboard-pie-leader-managed", !hasSlice);
                line?.remove();
                continue;
            }
            const x = point.cxFinal + point.pxmid[0];
            const y = point.cyFinal + point.pxmid[1];
            const start = new DOMPoint(x, y).matrixTransform(matrix);
            const box = label.getBoundingClientRect();
            if (!box.width || !box.height) continue;
            const middleX = (box.left + box.right) / 2;
            const middleY = (box.top + box.bottom) / 2;
            const horizontal = start.x <= box.left || start.x >= box.right;
            const direction = (horizontal ? start.x <= box.left : start.y <= middleY) ? 1 : -1;
            const gap = 3;
            const end = horizontal
                ? new DOMPoint((direction > 0 ? box.left : box.right) - direction * gap, middleY)
                : new DOMPoint(middleX, (direction > 0 ? box.top : box.bottom) - direction * gap);
            const lead = Math.min(9, Math.abs(horizontal ? end.x - start.x : end.y - start.y) / 2);
            const elbow = horizontal
                ? new DOMPoint(end.x - direction * lead, end.y)
                : new DOMPoint(end.x, end.y - direction * lead);
            const inverse = matrix.inverse();
            const points = [start, elbow, end].map(point => {
                const local = point.matrixTransform(inverse);
                return local.x + "," + local.y;
            }).join(" ");
            if (!line) {
                line = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
                line.setAttribute("class", leaderClass);
                line.setAttribute("fill", "none");
                line.setAttribute("stroke", "#333333");
                line.setAttribute("stroke-width", "1.25");
                line.setAttribute("stroke-linecap", "round");
                line.setAttribute("stroke-linejoin", "round");
                line.setAttribute("pointer-events", "none");
                line.setAttribute("aria-hidden", "true");
                slice.append(line);
            }
            line.setAttribute("points", points);
            slice.classList.add("dashboard-pie-leader-managed");
        }
    }

    function redraw() {
        if (framePending) return;
        framePending = true;
        requestAnimationFrame(drawLeaders);
    }

    new MutationObserver(records => {
        if (records.some(record => {
            if (record.target.closest?.("." + leaderClass)) return false;
            if (record.type === "attributes") return !!record.target.closest?.(".pielayer");
            return [...record.addedNodes, ...record.removedNodes].some(node =>
                node.nodeType === 1 && !node.classList.contains(leaderClass) &&
                (node.closest(".pielayer") || node.querySelector(".pielayer"))
            );
        })) redraw();
    }).observe(document.body, {
        subtree: true,
        childList: true,
        attributes: true,
        attributeFilter: ["d", "transform", "style"],
    });
    window.addEventListener("resize", redraw);
    redraw();
})();
</script>''',
    unsafe_allow_javascript=True,
)


def black_caption(container, body):
    return container.caption(
        f'<span class="dashboard-caption-text" style="color: #000000">{escape(body)}</span>',
        unsafe_allow_html=True,
    )


DATA = Path(__file__).parent / "data"


# ============================================================
# 1. DATA LOADING - CLICKHOUSE GOLD
# ============================================================

# Nạp biến môi trường từ file .env cạnh app.py (nếu có).
# Khi chạy bằng Docker Compose, các biến môi trường đã được inject vẫn được ưu tiên.
load_dotenv()
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv(
    "CLICKHOUSE_HTTP_PORT",
    os.getenv("EXTERNAL_CLICKHOUSE_HTTP_PORT", os.getenv("CLICKHOUSE_PORT", "8123"))
))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "olist_gold_seller")

# Danh sách bảng Gold mới, tên khớp với init_gold.sql và iceberg.gold.
GOLD_TABLE_NAMES = [
    "sales_detail", "orders", "payments_by_order", "reviews",
    "seller_order", "customer_order_bridge",
    "sales_performance_daily", "sales_performance_monthly",
    "category_performance_daily", "category_performance_monthly",
    "product_performance_monthly",
    "geography_performance_daily", "geography_performance_monthly",
    "order_status_daily", "order_status_monthly",
    "delivery_performance_daily", "delivery_performance_monthly",
    "order_hourly_daily", "order_hourly_monthly", "order_weekday_monthly",
    "review_score_daily", "review_score_monthly",
    "review_geography_monthly", "review_category_monthly",
    "customer_lifetime", "customer_activity_monthly", "customer_activity_daily",
]


@st.cache_resource(show_spinner=False)
def get_clickhouse_client():
    """Tạo ClickHouse HTTP client dùng chung cho toàn bộ phiên Streamlit."""
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
        connect_timeout=15,
        send_receive_timeout=300,
    )


@st.cache_data(show_spinner="Đang nạp dữ liệu từ ClickHouse...")
def load_data():
    client = get_clickhouse_client()

    def read_table(table_name, required=True):
        try:
            return client.query_df(f"SELECT * FROM `{CLICKHOUSE_DATABASE}`.`{table_name}`")
        except Exception:
            if required:
                raise
            return pd.DataFrame()

    # Bảng chi tiết mới thay cho gold_sales_detail.parquet.
    d = read_table("sales_detail")
    # Bảng đơn hàng ở grain order thay cho gold_orders.parquet.
    orders = read_table("orders")
    # App chỉ cần seller_id để tạo bộ lọc shop; lấy trực tiếp từ sales_detail.
    sellers = (
        d[["seller_id"]].dropna().drop_duplicates().reset_index(drop=True)
        if "seller_id" in d.columns else pd.DataFrame(columns=["seller_id"])
    )
    # Các biến này được giữ để không làm thay đổi cấu trúc và luồng xử lý hiện tại.
    # Thông tin sản phẩm đã có sẵn trong sales_detail; customer_order không được
    # dùng trực tiếp trong các trang dashboard hiện tại.
    product_cols = [c for c in ["product_key", "product_id"] if c in d.columns]
    products = d[product_cols].drop_duplicates().reset_index(drop=True) if product_cols else pd.DataFrame()
    customer_order = read_table("customer_lifetime", required=False)
    reviews = read_table("reviews", required=False)

    # ClickHouse đang đặt timezone UTC. Chuẩn hóa timestamp về UTC-naive giống app cũ.
    for frame in (d, orders, reviews):
        if frame is not None and not frame.empty:
            for col in frame.columns:
                if col.endswith("_timestamp") or col.endswith("_date"):
                    if pd.api.types.is_datetime64_any_dtype(frame[col]):
                        frame[col] = pd.to_datetime(
                            frame[col], errors="coerce", utc=True
                        ).dt.tz_localize(None)

    for frame in (d, orders):
        if frame is None or frame.empty:
            continue
        for col in ["purchase_date", "month"]:
            if col in frame.columns:
                frame[col] = pd.to_datetime(frame[col], errors="coerce")
        if "order_purchase_timestamp" in frame.columns:
            frame["order_purchase_timestamp"] = pd.to_datetime(
                frame["order_purchase_timestamp"], errors="coerce", utc=True
            ).dt.tz_localize(None)

    for c in [
        "is_delivered", "is_canceled", "delivery_days", "delivery_delay_days",
        "estimated_delivery_days", "review_score", "price", "freight_value", "revenue"
    ]:
        for frame in (d, orders):
            if frame is not None and c in frame.columns:
                frame[c] = pd.to_numeric(frame[c], errors="coerce")

    return d, orders, sellers, products, customer_order, reviews


try:
    d, orders, sellers, products, customer_order, reviews = load_data()
except Exception as exc:
    st.error(
        "Không thể nạp dữ liệu từ ClickHouse. "
        "Hãy kiểm tra ClickHouse đang chạy, thông tin kết nối và các bảng trong database olist_gold."
    )
    st.exception(exc)
    st.stop()


@st.cache_data(show_spinner=False)
def load_gold_marts():
    """Lấy danh sách bảng Gold hiện có; không tải lại dữ liệu mart không cần thiết."""
    client = get_clickhouse_client()
    result = client.query_df(
        "SELECT name FROM system.tables "
        "WHERE database = {database:String}",
        parameters={"database": CLICKHOUSE_DATABASE},
    )
    existing = set(result["name"].astype(str)) if not result.empty else set()
    return {name: None for name in GOLD_TABLE_NAMES if name in existing}


GOLD_MARTS = load_gold_marts()

# Chỉ đơn giao thành công được đưa vào mọi phép phân tích kinh doanh.
# Giữ bản gốc riêng cho biểu đồ trạng thái và tỷ lệ hủy (chỉ số trạng thái).
all_sales_detail = d
order_status_source = orders.copy()
if "order_id" not in orders.columns or "order_status" not in orders.columns:
    st.error("Bảng orders thiếu order_id hoặc order_status; không thể xác định đơn giao thành công.")
    st.stop()
valid_delivered_ids = set(
    orders.loc[
        orders["order_status"].astype(str).str.strip().str.lower().eq("delivered")
        & pd.to_numeric(orders.get("is_delivered", pd.Series(1, index=orders.index)), errors="coerce").fillna(0).eq(1)
        & pd.to_numeric(orders.get("is_canceled", pd.Series(0, index=orders.index)), errors="coerce").fillna(0).ne(1),
        "order_id"
    ].dropna()
)
d = d[d["order_id"].isin(valid_delivered_ids)].copy()
orders = orders[orders["order_id"].isin(valid_delivered_ids)].copy()
# Danh sách shop vẫn lấy từ toàn bộ giao dịch để không làm biến mất shop chưa giao thành công.
sellers = all_sales_detail[["seller_id"]].dropna().drop_duplicates().reset_index(drop=True)


# ============================================================
# 2. SIDEBAR AND COMMON HELPERS
# ============================================================

st.sidebar.title("Quản lý cửa hàng")
if st.sidebar.button("Làm mới dữ liệu"):
    st.cache_data.clear()
    st.rerun()

page = st.sidebar.radio(
    "**Phân hệ**",
    [
        "Tổng quan doanh thu & sản phẩm",
        "Tình hình kinh doanh tháng",
        "Tình hình kinh doanh ngày",
        "Khách hàng và địa lý",
        "Đơn hàng và vận hành"
    ]
)
black_caption(st.sidebar, "Dữ liệu Olist · ClickHouse Gold")
black_caption(st.sidebar, f"Đã kết nối {len(GOLD_MARTS):,} bảng Gold trên ClickHouse")

if all_sales_detail.empty:
    st.warning("Không có dữ liệu bán hàng trong ClickHouse.")
    st.stop()

min_ts = all_sales_detail["order_purchase_timestamp"].min()
max_ts = all_sales_detail["order_purchase_timestamp"].max()
if pd.isna(min_ts) or pd.isna(max_ts):
    st.error("Không xác định được khoảng thời gian vì order_purchase_timestamp không hợp lệ.")
    st.stop()

min_date = min_ts.date()
max_date = max_ts.date()

date_range = st.sidebar.date_input(
    "**Khoảng thời gian**",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
    start, end = date_range
else:
    start, end = min_date, max_date

shop_options = sorted(sellers["seller_id"].dropna().astype(str).unique().tolist())
if not shop_options:
    st.error("Không tìm thấy shop trong dim_seller.")
    st.stop()

selected_shop = st.sidebar.selectbox(
    "**Chọn shop cần phân tích**",
    shop_options,
    index=0
)

# Dữ liệu đầy đủ của shop, không phụ thuộc bộ lọc ngày toàn cục.
shop_all = d[d["seller_id"].astype(str) == selected_shop].copy()
shop_all_status = all_sales_detail[all_sales_detail["seller_id"].astype(str) == selected_shop].copy()
status_f = shop_all_status[
    (shop_all_status["order_purchase_timestamp"].dt.date >= start) &
    (shop_all_status["order_purchase_timestamp"].dt.date <= end)
].copy()

# Các trang phân tích thông thường vẫn sử dụng bộ lọc ngày chung.
f = shop_all[
    (shop_all["order_purchase_timestamp"].dt.date >= start) &
    (shop_all["order_purchase_timestamp"].dt.date <= end)
].copy()

black_caption(st.sidebar, f"Shop đang phân tích: {selected_shop}")
black_caption(st.sidebar, f"{len(f):,} dòng sản phẩm - đơn hàng trong bộ lọc ngày")


def money(value):
    return f"R$ {value:,.0f}"


def empty(message):
    st.info(message)


def monthly(df, metric="revenue"):
    return (
        df.groupby("month", as_index=False)[metric]
        .sum()
        .sort_values("month")
    )


def plotly_chart(container, fig, **kwargs):
    text_color = "#000000"
    fig.update_layout(
        font_color=text_color,
        title_font_color=text_color,
        legend_font_color=text_color,
        legend_title_font_color=text_color,
        hoverlabel=dict(bgcolor="#FFFFFF", font_color=text_color),
    )
    fig.update_xaxes(tickfont_color=text_color, title_font_color=text_color)
    fig.update_yaxes(tickfont_color=text_color, title_font_color=text_color)
    fig.update_coloraxes(
        colorbar_tickfont_color=text_color,
        colorbar_title_font_color=text_color,
    )
    for trace_type in ("bar", "pie"):
        fig.update_traces(
            selector=dict(type=trace_type),
            textposition="outside",
            insidetextfont_color=text_color,
            outsidetextfont_color=text_color,
        )
    return container.plotly_chart(fig, **kwargs)


def chart_line(df, x, y, title):
    if df.empty:
        empty("Không có dữ liệu để vẽ biểu đồ.")
        return
    fig = px.line(df, x=x, y=y, markers=True, title=title)
    fig.update_layout(
        hovermode="x unified",
        margin=dict(l=10, r=10, t=50, b=10)
    )
    plotly_chart(st, fig, use_container_width=True)


def fmt_axis(fig, y_title=None):
    fig.update_layout(
        margin=dict(l=10, r=10, t=45, b=10),
        legend_title_text="",
        yaxis_title=y_title
    )
    return fig

def complete_time_series(df, date_col, value_col, start_date, end_date, freq="D"):
    """Bổ sung ngày/tháng thiếu bằng 0, đồng nhất múi giờ trước khi reindex."""
    # CSV Olist có thể lưu timestamp kèm +00:00. Nếu đem index có timezone
    # reindex bằng calendar không timezone, pandas không khớp ngày và trả toàn số 0.
    # Chuẩn hóa cả dữ liệu lẫn lịch về datetime không timezone (UTC).
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    if start_ts.tzinfo is not None:
        start_ts = start_ts.tz_convert("UTC").tz_localize(None)
    if end_ts.tzinfo is not None:
        end_ts = end_ts.tz_convert("UTC").tz_localize(None)

    def parse_naive(values):
        return pd.to_datetime(values, errors="coerce", utc=True).dt.tz_localize(None)

    if freq == "M":
        calendar = pd.period_range(
            start_ts.to_period("M"), end_ts.to_period("M"), freq="M"
        ).to_timestamp()
        def convert(values):
            return parse_naive(values).dt.to_period("M").dt.to_timestamp()
    else:
        calendar = pd.date_range(
            start_ts.normalize(), end_ts.normalize(), freq="D"
        )
        def convert(values):
            return parse_naive(values).dt.normalize()

    if df.empty:
        values = pd.Series(0.0, index=calendar)
    else:
        temp = df.copy()
        temp[date_col] = convert(temp[date_col])
        values = (
            temp.dropna(subset=[date_col])
            .groupby(date_col)[value_col]
            .sum()
            .reindex(calendar, fill_value=0)
        )
    return values.rename_axis(date_col).reset_index(name=value_col)



# ============================================================
# 3. DASHBOARD PAGES
# ============================================================

st.title("Phân tích kinh doanh cửa hàng")
black_caption(
    st,
    f"Báo cáo hoạt động của shop {selected_shop} · "
    "Dữ liệu Olist Gold trên ClickHouse · Đơn vị tiền tệ: BRL"
)

if page == "Tổng quan doanh thu & sản phẩm":
    st.subheader("Tổng quan doanh thu và sản phẩm")
    if f.empty:
        st.info("Không phát sinh đơn hàng trong khoảng thời gian đã chọn. Các chỉ số được hiển thị bằng 0.")
        z = st.columns(4)
        z[0].metric("Doanh thu tháng gần nhất", money(0))
        z[1].metric("Tổng doanh thu trong kỳ", money(0))
        z[2].metric("Số đơn hàng trong kỳ", "0")
        z[3].metric("Số lượng sản phẩm đã bán", "0")
        st.markdown("### Tình hình doanh thu")
        zero_monthly = complete_time_series(
            pd.DataFrame(columns=["thang", "doanh_thu"]),
            "thang", "doanh_thu", start, end, freq="M"
        ).rename(columns={"thang": "Tháng", "doanh_thu": "Doanh thu"})
        chart_line(zero_monthly, "Tháng", "Doanh thu", "Doanh thu theo tháng")
        zero_daily = complete_time_series(
            pd.DataFrame(columns=["ngay", "doanh_thu"]),
            "ngay", "doanh_thu", start, end, freq="D"
        ).rename(columns={"ngay": "Ngày", "doanh_thu": "Doanh thu"})
        chart_line(
            zero_daily, "Ngày", "Doanh thu",
            "Doanh thu theo ngày trong khoảng thời gian đã chọn"
        )
        st.markdown("### Doanh thu theo danh mục sản phẩm")
        st.info("Doanh thu, số lượng bán và số đơn hàng của tất cả danh mục: 0.")
        st.markdown("### Sản phẩm nổi bật")
        st.info("Số lượng bán và doanh thu của các sản phẩm: 0.")
    else:
        latest_ts = f["order_purchase_timestamp"].max()
        latest_month = latest_ts.to_period("M")
        month_data = f[f["order_purchase_timestamp"].dt.to_period("M") == latest_month]
        month_label = latest_month.strftime("%m/%Y")

        kpi = st.columns(4)
        kpi[0].metric("Doanh thu tháng gần nhất", money(month_data["revenue"].sum()))
        kpi[1].metric("Tổng doanh thu trong kỳ", money(f["revenue"].sum()))
        kpi[2].metric("Số đơn hàng trong kỳ", f"{f['order_id'].nunique():,}")
        kpi[3].metric("Số lượng sản phẩm đã bán", f"{len(f):,}")
        black_caption(
            st,
            f"Tháng gần nhất có dữ liệu: {month_label}. "
            "Tổng doanh thu được tính theo toàn bộ khoảng thời gian đang chọn."
        )

        st.divider()
        st.markdown("### Tình hình doanh thu")
        monthly_data = (
            f.assign(thang=f["order_purchase_timestamp"].dt.to_period("M").dt.to_timestamp())
            .groupby("thang", as_index=False)
            .agg(doanh_thu=("revenue", "sum"))
        )
        monthly_data = complete_time_series(
            monthly_data, "thang", "doanh_thu", start, end, freq="M"
        ).rename(columns={"thang": "Tháng", "doanh_thu": "Doanh thu"})
        chart_line(monthly_data, "Tháng", "Doanh thu", "Doanh thu theo tháng")

        daily_data = (
            f.assign(ngay=f["order_purchase_timestamp"].dt.normalize())
            .groupby("ngay", as_index=False)
            .agg(doanh_thu=("revenue", "sum"))
        )
        daily_data = complete_time_series(
            daily_data, "ngay", "doanh_thu", start, end, freq="D"
        ).rename(columns={"ngay": "Ngày", "doanh_thu": "Doanh thu"})
        chart_line(
            daily_data, "Ngày", "Doanh thu",
            "Doanh thu theo ngày trong khoảng thời gian đã chọn"
        )

        st.divider()
        st.markdown("### Doanh thu theo danh mục sản phẩm")
        category_data = (
            f.groupby("product_category_name_english", as_index=False)
            .agg(
                doanh_thu=("revenue", "sum"),
                so_luong=("item_key", "count"),
                so_don=("order_id", "nunique")
            )
            .sort_values("doanh_thu", ascending=False)
        )
        if not category_data.empty:
            category_data = category_data.rename(columns={
                "product_category_name_english": "Danh mục",
                "doanh_thu": "Doanh thu (BRL)",
                "so_luong": "Số lượng bán",
                "so_don": "Số đơn hàng"
            })
            fig = px.bar(
                category_data.head(15), x="Doanh thu (BRL)", y="Danh mục",
                orientation="h", title="Doanh thu theo danh mục trong kỳ"
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            plotly_chart(st, fig, use_container_width=True)
            st.dataframe(category_data, use_container_width=True, hide_index=True)
        else:
            st.info("Số liệu danh mục trong khoảng thời gian đã chọn: 0 doanh thu, 0 sản phẩm, 0 đơn hàng.")

        st.divider()
        st.markdown("### Sản phẩm nổi bật")
        period_products = (
            f.groupby(["product_key", "product_id"], as_index=False)
            .agg(
                so_luong=("item_key", "count"),
                so_don=("order_id", "nunique"),
                doanh_thu=("revenue", "sum"),
                gia_tb=("price", "mean")
            )
        )
        top_units = period_products.nlargest(10, "so_luong")
        top_revenue = (
            f.groupby(["product_key", "product_id"], as_index=False)
            .agg(
                doanh_thu=("revenue", "sum"),
                so_luong=("item_key", "count"),
                so_don=("order_id", "nunique")
            )
            .nlargest(10, "doanh_thu")
        )
        left, right = st.columns(2)
        if not top_units.empty:
            fig_units = px.bar(
                top_units.sort_values("so_luong"), x="so_luong", y="product_id",
                orientation="h", title="Top sản phẩm bán nhiều nhất trong kỳ",
                labels={"so_luong": "Số lượng bán", "product_id": "Mã sản phẩm"}
            )
            plotly_chart(left, fig_units, use_container_width=True)
        else:
            left.info("Không có dữ liệu sản phẩm trong tháng gần nhất.")

        if not top_revenue.empty:
            fig_revenue = px.bar(
                top_revenue.sort_values("doanh_thu"), x="doanh_thu", y="product_id",
                orientation="h", title="Top sản phẩm theo doanh thu trong kỳ",
                labels={"doanh_thu": "Doanh thu (BRL)", "product_id": "Mã sản phẩm"}
            )
            plotly_chart(right, fig_revenue, use_container_width=True)
        else:
            right.info("Không có dữ liệu sản phẩm.")

        st.markdown("#### Bảng sản phẩm nổi bật trong khoảng thời gian đã chọn")
        product_table = (
            f.groupby(["product_key", "product_id", "product_category_name_english"], as_index=False, dropna=False)
            .agg(
                so_luong=("item_key", "count"),
                so_don=("order_id", "nunique"),
                gia_san_pham=("price", "mean"),
                doanh_thu=("revenue", "sum")
            )
            .sort_values("doanh_thu", ascending=False)
            [["product_id", "product_category_name_english", "so_luong", "so_don", "gia_san_pham", "doanh_thu"]]
            .rename(columns={
                "product_id": "Mã sản phẩm",
                "product_category_name_english": "Danh mục sản phẩm",
                "so_luong": "Số lượng bán",
                "so_don": "Số đơn đặt hàng",
                "gia_san_pham": "Giá sản phẩm trung bình (BRL)",
                "doanh_thu": "Doanh thu (BRL)"
            })
        )
        st.dataframe(
            product_table, use_container_width=True, hide_index=True,
            column_config={
                "Giá sản phẩm trung bình (BRL)": st.column_config.NumberColumn(format="R$ %.2f"),
                "Doanh thu (BRL)": st.column_config.NumberColumn(format="R$ %.2f")
            }
        )


elif page == "Tình hình kinh doanh tháng":
    st.subheader("Tình hình kinh doanh tháng")
    black_caption(
        st,
        "Báo cáo sử dụng toàn bộ dữ liệu của tháng được chọn trong lịch sử của shop, "
        "không bị giới hạn bởi bộ lọc khoảng thời gian ở thanh bên."
    )

    monthly_source = shop_all.dropna(
        subset=["order_purchase_timestamp"]
    ).copy()

    if monthly_source.empty:
        monthly_source = shop_all.iloc[0:0].copy()
    if not d["order_purchase_timestamp"].dropna().empty:
        # Lịch tháng lấy theo toàn bộ dataset, kể cả tháng shop không có đơn.
        global_dates = all_sales_detail["order_purchase_timestamp"].dropna()
        global_min = global_dates.min().to_period("M")
        global_max = global_dates.max().to_period("M")
        available_months = list(pd.period_range(global_min, global_max, freq="M"))
        available_years = sorted({period.year for period in available_months})

        month_names = {
            1: "Tháng 1", 2: "Tháng 2", 3: "Tháng 3",
            4: "Tháng 4", 5: "Tháng 5", 6: "Tháng 6",
            7: "Tháng 7", 8: "Tháng 8", 9: "Tháng 9",
            10: "Tháng 10", 11: "Tháng 11", 12: "Tháng 12"
        }

        latest_available = max(available_months)
        selector_left, selector_right = st.columns([1, 2])
        with selector_left:
            selected_year = st.selectbox(
                "Chọn năm",
                available_years,
                index=available_years.index(latest_available.year),
                key="monthly_report_year"
            )

        months_in_year = sorted(
            period.month for period in available_months
            if period.year == selected_year
        )
        with selector_right:
            selected_month = st.selectbox(
                "Chọn tháng",
                months_in_year,
                index=months_in_year.index(
                    latest_available.month if latest_available.year == selected_year
                    else months_in_year[-1]
                ),
                format_func=lambda value: month_names[value],
                key="monthly_report_month"
            )

        selected_period = pd.Period(
            year=int(selected_year), month=int(selected_month), freq="M"
        )
        month_start = selected_period.start_time
        month_end = selected_period.end_time

        month_data = monthly_source[
            monthly_source["order_purchase_timestamp"].dt.to_period("M")
            == selected_period
        ].copy()

        if month_data.empty:
            st.info(
                "Shop không phát sinh đơn hàng trong tháng này. "
                "Doanh thu, đơn hàng, sản phẩm và khách hàng được tính bằng 0."
            )

        # Revenue and product statistics are calculated at item grain.
        month_revenue = float(month_data["revenue"].sum())
        month_orders_count = int(month_data["order_id"].nunique())
        month_units = int(month_data["item_key"].nunique())
        month_customers = int(month_data["customer_unique_id"].nunique())
        month_aov = month_revenue / month_orders_count if month_orders_count else 0
        month_avg_item_price = (
            month_data["price"].mean() if month_data["price"].notna().any() else 0
        )

        # Deduplicate order-level attributes before calculating order KPIs.
        month_order_ids = month_data["order_id"].dropna().unique()
        month_orders = orders[
            orders["order_id"].isin(month_order_ids)
        ].drop_duplicates("order_id").copy()

        delivered_orders = month_orders[month_orders["is_delivered"] == 1]
        status_month_ids_for_cancel = shop_all_status.loc[
            shop_all_status["order_purchase_timestamp"].dt.to_period("M") == selected_period,
            "order_id"
        ].dropna().unique()
        all_month_orders_for_cancel = order_status_source[
            order_status_source["order_id"].isin(status_month_ids_for_cancel)
        ].drop_duplicates("order_id")
        canceled_orders = all_month_orders_for_cancel[all_month_orders_for_cancel["is_canceled"] == 1]
        delivered_count = len(delivered_orders)
        canceled_count = len(canceled_orders)
        cancel_rate = (
            canceled_count / len(all_month_orders_for_cancel) * 100
            if len(month_orders) else 0
        )
        late_orders = delivered_orders[
            pd.to_numeric(
                delivered_orders["delivery_delay_days"], errors="coerce"
            ) > 0
        ]
        late_rate = (
            len(late_orders) / delivered_count * 100
            if delivered_count else 0
        )
        avg_delivery_days = (
            pd.to_numeric(delivered_orders["delivery_days"], errors="coerce").mean()
            if delivered_count else 0.0
        )
        avg_delay_days = (
            pd.to_numeric(delivered_orders["delivery_delay_days"], errors="coerce").mean()
            if delivered_count else 0.0
        )

        # Order-level review scores must not be counted once per product item.
        review_orders = (
            month_data.drop_duplicates("order_id")
            .dropna(subset=["review_score"])
        )
        avg_review = (
            review_orders["review_score"].mean()
            if not review_orders.empty else 0.0
        )
        review_count = int(review_orders["order_id"].nunique())

        st.markdown(
            f"### Báo cáo {month_names[int(selected_month)]} năm {selected_year}"
        )
        kpi1 = st.columns(4)
        kpi1[0].metric("Doanh thu hàng hóa", money(month_revenue))
        kpi1[1].metric("Số đơn hàng", f"{month_orders_count:,}")
        kpi1[2].metric("Số sản phẩm đã bán", f"{month_units:,}")
        kpi1[3].metric("Khách hàng", f"{month_customers:,}")

        kpi2 = st.columns(4)
        kpi2[0].metric("Giá trị trung bình/đơn", money(month_aov))
        kpi2[1].metric(
            "Tỷ lệ hủy đơn", f"{cancel_rate:.1f}%",
            help="Số đơn bị hủy chia cho tổng số đơn của shop trong tháng."
        )
        kpi2[2].metric(
            "Tỷ lệ giao trễ", f"{late_rate:.1f}%",
            help="Số đơn đã giao trễ chia cho số đơn đã giao thành công."
        )
        kpi2[3].metric(
            "Điểm đánh giá trung bình",
            f"{avg_review:.2f}/5"
        )

        # State the actual data coverage, especially for incomplete months.
        observed_min = month_data["order_purchase_timestamp"].min()
        observed_max = month_data["order_purchase_timestamp"].max()
        if pd.notna(observed_min) and pd.notna(observed_max):
            coverage_text = (
                f"Phạm vi dữ liệu thực tế trong tháng: "
                f"{observed_min:%d/%m/%Y} – {observed_max:%d/%m/%Y}. "
            )
        else:
            coverage_text = "Không phát sinh đơn hàng trong tháng được chọn. "
        black_caption(
            st,
            coverage_text
            + "Doanh thu là tổng giá trị price của sản phẩm; phí vận chuyển không cộng vào doanh thu."
        )

        # So sánh với tháng liền trước trên cùng phạm vi shop.
        previous_period = selected_period - 1
        previous_month_data = monthly_source[
            monthly_source["order_purchase_timestamp"].dt.to_period("M")
            == previous_period
        ].copy()
        previous_revenue = previous_month_data["revenue"].sum()
        previous_orders_count = previous_month_data["order_id"].nunique()
        previous_units = previous_month_data["item_key"].nunique()
        previous_customers_count = previous_month_data["customer_unique_id"].nunique()
        previous_aov = (
            previous_revenue / previous_orders_count
            if previous_orders_count else 0
        )

        def mom_delta(current, previous):
            return (
                (current - previous) / abs(previous) * 100
                if previous != 0 else None
            )

        st.divider()
        st.markdown("### So sánh với tháng trước")
        black_caption(
            st,
            f"So sánh {month_names[int(selected_month)]}/{selected_year} "
            f"với {month_names[previous_period.month]}/{previous_period.year}. "
            "Các chỉ số được tính riêng cho shop đang chọn."
        )
        mom_cols = st.columns(5)
        mom_metrics = [
            ("Doanh thu", month_revenue, previous_revenue, money),
            ("Số đơn hàng", month_orders_count, previous_orders_count, lambda x: f"{x:,.0f}"),
            ("Số lượng sản phẩm", month_units, previous_units, lambda x: f"{x:,.0f}"),
            ("Khách hàng", month_customers, previous_customers_count, lambda x: f"{x:,.0f}"),
            ("Giá trị TB/đơn", month_aov, previous_aov, money)
        ]
        for col, (label, current, previous, formatter) in zip(mom_cols, mom_metrics):
            delta = mom_delta(current, previous)
            col.metric(
                label, formatter(current),
                f"{delta:+.1f}% so với tháng trước"
                if delta is not None else "Tháng trước không phát sinh đơn hàng"
            )

        st.divider()
        st.markdown("### Diễn biến doanh thu trong tháng")
        daily_revenue = (
            month_data.assign(ngay=month_data["order_purchase_timestamp"].dt.normalize())
            .groupby("ngay", as_index=False)
            .agg(doanh_thu=("revenue", "sum"))
        )
        daily_revenue = complete_time_series(
            daily_revenue, "ngay", "doanh_thu",
            selected_period.start_time, selected_period.end_time, freq="D"
        ).rename(columns={"ngay": "Ngày", "doanh_thu": "Doanh thu"})
        chart_line(
            daily_revenue, "Ngày", "Doanh thu",
            f"Doanh thu từng ngày – {month_names[int(selected_month)]}/{selected_year}"
        )

        daily_orders = (
            month_orders.assign(
                ngay=month_orders["order_purchase_timestamp"].dt.normalize()
            )
            .groupby("ngay", as_index=False)
            .agg(so_don=("order_id", "nunique"))
        )
        daily_orders = complete_time_series(
            daily_orders, "ngay", "so_don",
            selected_period.start_time, selected_period.end_time, freq="D"
        ).rename(columns={"ngay": "Ngày", "so_don": "Số đơn hàng"})
        chart_line(
            daily_orders, "Ngày", "Số đơn hàng",
            "Số đơn hàng phát sinh theo ngày"
        )

        st.divider()
        st.markdown("### Cơ cấu sản phẩm và doanh thu")
        category_month = (
            month_data.groupby("product_category_name_english", as_index=False)
            .agg(
                doanh_thu=("revenue", "sum"),
                so_luong=("item_key", "nunique"),
                so_don=("order_id", "nunique")
            )
            .sort_values("doanh_thu", ascending=False)
            .rename(columns={
                "product_category_name_english": "Danh mục",
                "doanh_thu": "Doanh thu (BRL)",
                "so_luong": "Số lượng bán",
                "so_don": "Số đơn hàng"
            })
        )
        category_left, category_right = st.columns(2)
        if not category_month.empty:
            plotly_chart(
                category_left,
                px.bar(
                    category_month.head(10).sort_values("Doanh thu (BRL)"),
                    x="Doanh thu (BRL)", y="Danh mục",
                    orientation="h",
                    title="Top 10 danh mục theo doanh thu"
                ),
                use_container_width=True
            )
            plotly_chart(
                category_right,
                px.bar(
                    category_month.head(10).sort_values("Số lượng bán"),
                    x="Số lượng bán", y="Danh mục",
                    orientation="h",
                    title="Top 10 danh mục theo số lượng bán"
                ),
                use_container_width=True
            )
            with st.expander("Xem bảng doanh thu tất cả danh mục", expanded=False):
                st.dataframe(
                    category_month, use_container_width=True, hide_index=True
                )

        top_products_month = (
            month_data.groupby(["product_id"], as_index=False)
            .agg(
                so_luong=("item_key", "nunique"),
                so_don=("order_id", "nunique"),
                gia_trung_binh=("price", "mean"),
                doanh_thu=("revenue", "sum")
            )
            .sort_values("doanh_thu", ascending=False)
        )
        product_left, product_right = st.columns(2)
        top_by_units = top_products_month.nlargest(10, "so_luong").sort_values("so_luong")
        top_by_revenue = top_products_month.nlargest(10, "doanh_thu").sort_values("doanh_thu")

        plotly_chart(
            product_left,
            px.bar(
                top_by_units, x="so_luong", y="product_id",
                orientation="h",
                title="Top 10 sản phẩm bán nhiều nhất",
                labels={"so_luong": "Số lượng bán", "product_id": "Mã sản phẩm"}
            ),
            use_container_width=True
        )
        plotly_chart(
            product_right,
            px.bar(
                top_by_revenue, x="doanh_thu", y="product_id",
                orientation="h",
                title="Top 10 sản phẩm theo doanh thu",
                labels={"doanh_thu": "Doanh thu (BRL)", "product_id": "Mã sản phẩm"}
            ),
            use_container_width=True
        )

        st.markdown("#### Bảng sản phẩm trong tháng")
        product_table_month = top_products_month.rename(columns={
            "product_id": "Mã sản phẩm",
            "so_luong": "Số lượng bán",
            "so_don": "Số đơn hàng",
            "gia_trung_binh": "Giá sản phẩm trung bình (BRL)",
            "doanh_thu": "Doanh thu (BRL)"
        })
        st.dataframe(
            product_table_month,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Giá sản phẩm trung bình (BRL)": st.column_config.NumberColumn(format="R$ %.2f"),
                "Doanh thu (BRL)": st.column_config.NumberColumn(format="R$ %.2f")
            }
        )

        st.divider()
        st.markdown("### Trạng thái đơn hàng và hiệu quả giao hàng")
        status_month_order_ids = shop_all_status.loc[
            shop_all_status["order_purchase_timestamp"].dt.to_period("M") == selected_period,
            "order_id"
        ].dropna().unique()
        status_month_orders = order_status_source[
            order_status_source["order_id"].isin(status_month_order_ids)
        ].drop_duplicates("order_id")
        status_month = (
            status_month_orders["order_status"]
            .fillna("Không xác định")
            .value_counts()
            .rename_axis("Trạng thái")
            .reset_index(name="Số đơn")
        )
        status_left, status_right = st.columns(2)
        if not status_month.empty:
            plotly_chart(
                status_left,
                px.pie(
                    status_month, names="Trạng thái", values="Số đơn",
                    hole=0.45, title="Cơ cấu trạng thái đơn hàng"
                ),
                use_container_width=True
            )

        delivery_metrics = pd.DataFrame({
            "Chỉ tiêu": [
                "Số đơn đã giao thành công",
                "Số đơn bị hủy",
                "Số đơn giao trễ",
                "Thời gian giao hàng trung bình (ngày)",
                "Số ngày trễ trung bình (ngày)"
            ],
            "Giá trị": [
                delivered_count,
                canceled_count,
                len(late_orders),
                round(avg_delivery_days, 2) if pd.notna(avg_delivery_days) else np.nan,
                round(avg_delay_days, 2) if pd.notna(avg_delay_days) else np.nan
            ]
        })
        status_right.dataframe(
            delivery_metrics, use_container_width=True, hide_index=True
        )

        st.divider()
        st.markdown("### Tóm tắt kết quả kinh doanh")
        best_category = (
            category_month.iloc[0]["Danh mục"]
            if not category_month.empty else "Chưa có dữ liệu"
        )
        best_product_revenue = (
            top_products_month.iloc[0]["product_id"]
            if not top_products_month.empty else "Chưa có dữ liệu"
        )
        best_product_units = (
            top_products_month.sort_values("so_luong", ascending=False).iloc[0]["product_id"]
            if not top_products_month.empty else "Chưa có dữ liệu"
        )
        summary_left, summary_right = st.columns(2)
        summary_left.markdown(
            f"- **Danh mục mang lại doanh thu cao nhất:** {best_category}\n"
            f"- **Sản phẩm có doanh thu cao nhất:** `{best_product_revenue}`\n"
            f"- **Sản phẩm bán nhiều nhất:** `{best_product_units}`"
        )
        summary_right.markdown(
            f"- **Thời gian giao hàng trung bình:** "
            f"{avg_delivery_days:.1f} ngày" if pd.notna(avg_delivery_days)
            else "- **Thời gian giao hàng trung bình:** Chưa có dữ liệu"
        )
        summary_right.markdown(
            f"- **Tỷ lệ đơn hàng giao trễ:** {late_rate:.1f}%\n"
            f"- **Tỷ lệ đơn hàng bị hủy:** {cancel_rate:.1f}%\n"
            f"- **Điểm đánh giá trung bình:** "
            f"{avg_review:.2f}/5" if pd.notna(avg_review)
            else "- **Điểm đánh giá trung bình:** Chưa có dữ liệu"
        )


        st.divider()
        st.markdown("### Phân tích địa lý trong tháng")
        black_caption(
            st,
            "Khu vực được xác định theo bang (customer_state) của khách hàng. "
            "Doanh thu là tổng giá trị sản phẩm thuộc các đơn của shop."
        )

        if "customer_state" in month_data.columns:
            geo_month = (
                month_data.dropna(subset=["customer_state"])
                .groupby("customer_state", as_index=False)
                .agg(
                    revenue=("revenue", "sum"),
                    orders=("order_id", "nunique"),
                    customers=("customer_unique_id", "nunique")
                )
                .sort_values("revenue", ascending=False)
            )
            geo_previous = (
                previous_month_data.dropna(subset=["customer_state"])
                .groupby("customer_state", as_index=False)
                .agg(previous_revenue=("revenue", "sum"))
            )
            geo_compare = geo_month.merge(
                geo_previous, on="customer_state", how="left"
            ).fillna({"previous_revenue": 0})
            geo_compare["rank_current"] = (
                geo_compare["revenue"].rank(method="min", ascending=False).astype(int)
            )
            geo_compare["rank_previous"] = (
                geo_compare["previous_revenue"].rank(method="min", ascending=False)
            )
            geo_compare["rank_change"] = geo_compare.apply(
                lambda row: (
                    "Mới xuất hiện" if row["previous_revenue"] == 0
                    else (
                        f"Tăng {int(row['rank_previous'] - row['rank_current'])} bậc"
                        if row["rank_previous"] > row["rank_current"]
                        else (
                            f"Giảm {int(row['rank_current'] - row['rank_previous'])} bậc"
                            if row["rank_current"] > row["rank_previous"]
                            else "Không đổi"
                        )
                    )
                ), axis=1
            )
            geo_compare["revenue_change_pct"] = geo_compare.apply(
                lambda row: (
                    (row["revenue"] - row["previous_revenue"])
                    / row["previous_revenue"] * 100
                    if row["previous_revenue"] > 0 else np.nan
                ), axis=1
            )

            if not geo_compare.empty:
                top_state = geo_compare.iloc[0]
                geo_kpis = st.columns(3)
                geo_kpis[0].metric("Khu vực doanh thu cao nhất", str(top_state["customer_state"]))
                geo_kpis[1].metric("Doanh thu khu vực dẫn đầu", money(top_state["revenue"]))
                geo_kpis[2].metric("Khách hàng tại khu vực dẫn đầu", f"{int(top_state['customers']):,}")

                geo_left, geo_right = st.columns(2)
                plotly_chart(
                    geo_left,
                    px.bar(
                        geo_compare.head(10).sort_values("revenue"),
                        x="revenue", y="customer_state", orientation="h",
                        title="Top 10 khu vực theo doanh thu",
                        labels={"customer_state": "Bang khách hàng", "revenue": "Doanh thu (BRL)"}
                    ), use_container_width=True
                )
                rank_plot = geo_compare.head(10).copy()
                rank_plot["Thứ hạng tháng trước"] = rank_plot["rank_previous"].where(
                    rank_plot["previous_revenue"] > 0
                )
                rank_plot["Thứ hạng tháng này"] = rank_plot["rank_current"]
                rank_plot = rank_plot.melt(
                    id_vars=["customer_state"],
                    value_vars=["Thứ hạng tháng trước", "Thứ hạng tháng này"],
                    var_name="Kỳ báo cáo", value_name="Thứ hạng"
                )
                plotly_chart(
                    geo_right,
                    px.line(
                        rank_plot, x="customer_state", y="Thứ hạng",
                        color="Kỳ báo cáo", markers=True,
                        title="Biến động thứ hạng doanh thu khu vực",
                        labels={"customer_state": "Bang khách hàng", "Thứ hạng": "Thứ hạng (1 là cao nhất)"}
                    ).update_yaxes(autorange="reversed"),
                    use_container_width=True
                )

                geo_display = geo_compare.rename(columns={
                    "customer_state": "Khu vực / Bang",
                    "revenue": "Doanh thu tháng này (BRL)",
                    "previous_revenue": "Doanh thu tháng trước (BRL)",
                    "orders": "Số đơn", "customers": "Số khách hàng",
                    "rank_current": "Hạng tháng này",
                    "rank_previous": "Hạng tháng trước",
                    "rank_change": "Thay đổi thứ hạng",
                    "revenue_change_pct": "Tăng trưởng doanh thu (%)"
                })
                st.dataframe(
                    geo_display[[
                        "Khu vực / Bang", "Doanh thu tháng này (BRL)",
                        "Doanh thu tháng trước (BRL)", "Tăng trưởng doanh thu (%)",
                        "Số đơn", "Số khách hàng", "Hạng tháng này",
                        "Hạng tháng trước", "Thay đổi thứ hạng"
                    ]],
                    use_container_width=True, hide_index=True,
                    column_config={
                        "Doanh thu tháng này (BRL)": st.column_config.NumberColumn(format="R$ %.2f"),
                        "Doanh thu tháng trước (BRL)": st.column_config.NumberColumn(format="R$ %.2f"),
                        "Tăng trưởng doanh thu (%)": st.column_config.NumberColumn(format="%.1f%%")
                    }
                )
            else:
                empty("Không có thông tin bang khách hàng trong tháng này.")
        else:
            empty("Dữ liệu hiện tại không có cột customer_state để phân tích địa lý.")

        st.divider()
        # Customer movement: compare the selected month with the immediately
        # preceding calendar month for this seller only.
        customer_history = monthly_source.dropna(
            subset=["customer_unique_id", "order_purchase_timestamp"]
        ).copy()
        customer_history["customer_unique_id"] = (
            customer_history["customer_unique_id"].astype(str)
        )
        current_customer_ids = set(
            customer_history.loc[
                customer_history["order_purchase_timestamp"].dt.to_period("M")
                == selected_period,
                "customer_unique_id"
            ].unique()
        )
        previous_period = selected_period - 1
        previous_customer_ids = set(
            customer_history.loc[
                customer_history["order_purchase_timestamp"].dt.to_period("M")
                == previous_period,
                "customer_unique_id"
            ].unique()
        )
        earlier_customer_ids = set(
            customer_history.loc[
                customer_history["order_purchase_timestamp"].dt.to_period("M")
                < selected_period,
                "customer_unique_id"
            ].unique()
        )

        customers_bought = len(current_customer_ids)
        customers_previous = len(previous_customer_ids)
        customers_retained = len(current_customer_ids & previous_customer_ids)
        customers_churned = len(previous_customer_ids - current_customer_ids)
        customers_new = len(current_customer_ids - earlier_customer_ids)
        customers_returning = customers_bought - customers_new
        customer_churn_rate = (
            customers_churned / customers_previous * 100
            if customers_previous else 0
        )

        st.divider()
        st.markdown("### Khách hàng mua hàng và khách hàng rời bỏ")
        black_caption(
            st,
            f"So sánh khách hàng của {month_names[int(selected_month)]}/{selected_year} "
            f"với tháng liền trước ({month_names[previous_period.month]}/{previous_period.year}). "
            "Khách hàng rời bỏ trong báo cáo này được xác định là khách đã mua ở tháng trước "
            "nhưng không phát sinh đơn ở tháng đang xem; đây là chỉ báo rời bỏ ngắn hạn, "
            "không phải kết luận khách hàng đã rời bỏ vĩnh viễn."
        )

        customer_kpis = st.columns(4)
        customer_kpis[0].metric(
            "Khách hàng mua trong tháng", f"{customers_bought:,}"
        )
        customer_kpis[1].metric(
            "Khách hàng mới", f"{customers_new:,}"
        )
        customer_kpis[2].metric(
            "Khách hàng quay lại", f"{customers_returning:,}"
        )
        customer_kpis[3].metric(
            "Khách hàng rời bỏ", f"{customers_churned:,}",
            delta=f"{customer_churn_rate:.1f}% so với tháng trước",
            delta_color="inverse",
            help="Tỷ lệ rời bỏ = khách mua tháng trước nhưng không mua tháng này / tổng khách mua tháng trước."
        )

        customer_summary = pd.DataFrame({
            "Chỉ tiêu khách hàng": [
                "Khách hàng mua trong tháng",
                "Khách hàng mua tháng trước",
                "Khách hàng được giữ chân (mua cả hai tháng)",
                "Khách hàng mới (chưa từng mua trước tháng này)",
                "Khách hàng quay lại (đã từng mua trước đó)",
                "Khách hàng rời bỏ so với tháng trước",
                "Tỷ lệ rời bỏ ngắn hạn (%)"
            ],
            "Số lượng / Tỷ lệ": [
                customers_bought,
                customers_previous,
                customers_retained,
                customers_new,
                customers_returning,
                customers_churned,
                round(customer_churn_rate, 2)
            ]
        })
        customer_left, customer_right = st.columns([1, 1.4])
        customer_left.dataframe(
            customer_summary, use_container_width=True, hide_index=True
        )
        customer_chart_data = pd.DataFrame({
            "Nhóm khách hàng": [
                "Khách hàng mới",
                "Khách hàng quay lại",
                "Khách hàng rời bỏ"
            ],
            "Số lượng": [
                customers_new,
                customers_returning,
                customers_churned
            ]
        })
        plotly_chart(
            customer_right,
            px.bar(
                customer_chart_data,
                x="Nhóm khách hàng",
                y="Số lượng",
                title="Biến động khách hàng trong tháng",
                text="Số lượng"
            ),
            use_container_width=True
        )

        # Các phân tích vận hành bổ sung, giới hạn trong tháng và shop đang chọn.
        st.divider()
        st.markdown("### Số lượng đơn hàng theo giờ trong ngày")
        if not month_orders.empty:
            hourly_month = month_orders.dropna(
                subset=["order_purchase_timestamp"]
            ).copy()
            hourly_month["purchase_hour"] = (
                hourly_month["order_purchase_timestamp"].dt.hour
            )
            hourly_month_summary = (
                hourly_month.groupby("purchase_hour")["order_id"]
                .nunique()
                .reindex(range(24), fill_value=0)
                .rename("orders")
                .reset_index()
            )
        else:
            hourly_month_summary = pd.DataFrame({
                "purchase_hour": range(24), "orders": [0] * 24
            })

        if not hourly_month_summary.empty:
            peak_hour = hourly_month_summary.loc[
                hourly_month_summary["orders"].idxmax()
            ]
        else:
            peak_hour = {"purchase_hour": 0, "orders": 0}
        hm1, hm2 = st.columns([3, 1])
        plotly_chart(
            hm1,
            px.bar(
                hourly_month_summary,
                x="purchase_hour",
                y="orders",
                title="Đơn hàng phát sinh theo từng giờ",
                labels={
                    "purchase_hour": "Giờ trong ngày",
                    "orders": "Số đơn hàng"
                },
                text="orders"
            ),
            use_container_width=True
        )
        hm2.metric(
            "Khung giờ cao điểm",
            f"{int(peak_hour['purchase_hour']):02d}:00–"
            f"{(int(peak_hour['purchase_hour']) + 1) % 24:02d}:00",
            f"{int(peak_hour['orders']):,} đơn"
        )

        st.divider()
        st.markdown("### So sánh số lượng đơn hàng theo ngày trong tuần")
        weekday_labels = {
            0: "Thứ 2", 1: "Thứ 3", 2: "Thứ 4", 3: "Thứ 5",
            4: "Thứ 6", 5: "Thứ 7", 6: "Chủ nhật"
        }
        if not month_orders.empty:
            weekday_month = month_orders.dropna(
                subset=["order_purchase_timestamp"]
            ).copy()
            weekday_month["weekday_num"] = (
                weekday_month["order_purchase_timestamp"].dt.dayofweek
            )
            weekday_month_summary = (
                weekday_month.groupby("weekday_num")["order_id"]
                .nunique()
                .reindex(range(7), fill_value=0)
                .rename("orders")
                .reset_index()
            )
        else:
            weekday_month_summary = pd.DataFrame({
                "weekday_num": range(7), "orders": [0] * 7
            })
        weekday_month_summary["day_name"] = (
            weekday_month_summary["weekday_num"].map(weekday_labels)
        )
        weekday_peak = weekday_month_summary.loc[
            weekday_month_summary["orders"].idxmax()
        ]
        wm1, wm2 = st.columns([3, 1])
        plotly_chart(
            wm1,
            px.bar(
                weekday_month_summary,
                x="day_name",
                y="orders",
                title="Số đơn hàng theo thứ trong tuần",
                category_orders={"day_name": [
                    "Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5",
                    "Thứ 6", "Thứ 7", "Chủ nhật"
                ]},
                labels={
                    "day_name": "Ngày trong tuần",
                    "orders": "Số đơn hàng"
                },
                text="orders"
            ),
            use_container_width=True
        )
        wm2.metric(
            "Ngày có nhiều đơn nhất",
            str(weekday_peak["day_name"]),
            f"{int(weekday_peak['orders']):,} đơn"
        )

        st.divider()
        st.markdown("### Đánh giá của khách hàng")
        black_caption(
            st,
            "Đánh giá được tính theo các đơn hàng của shop trong tháng đang xem. "
            "Mỗi đơn hàng chỉ được tính một lần."
        )
        if (
            reviews is not None
            and not reviews.empty
            and "order_id" in reviews.columns
            and not month_orders.empty
        ):
            month_reviews = reviews[
                reviews["order_id"].astype(str).isin(
                    month_orders["order_id"].astype(str)
                )
            ].copy()
            if "review_score" in month_reviews.columns:
                month_reviews["review_score"] = pd.to_numeric(
                    month_reviews["review_score"], errors="coerce"
                )
                month_reviews = month_reviews.dropna(
                    subset=["review_score"]
                ).drop_duplicates(subset=["order_id"], keep="last")
            else:
                month_reviews = pd.DataFrame()

        else:
            month_reviews = pd.DataFrame()

        if not month_reviews.empty and "review_score" in month_reviews.columns:
            month_rating_counts = (
                month_reviews.groupby("review_score")["order_id"]
                .nunique()
                .reindex([1, 2, 3, 4, 5], fill_value=0)
                .rename("orders")
                .reset_index()
            )
            month_average_score = month_reviews["review_score"].mean()
            month_rated_count = month_reviews["order_id"].nunique()
            month_negative_count = month_reviews.loc[
                month_reviews["review_score"] <= 2, "order_id"
            ].nunique()
        else:
            month_rating_counts = pd.DataFrame({
                "review_score": [1, 2, 3, 4, 5],
                "orders": [0, 0, 0, 0, 0]
            })
            month_average_score = 0.0
            month_rated_count = 0
            month_negative_count = 0

        mr_kpis = st.columns(3)
        mr_kpis[0].metric(
            "Điểm đánh giá trung bình",
            f"{month_average_score:.2f}/5"
        )
        mr_kpis[1].metric("Đơn được đánh giá", f"{month_rated_count:,}")
        mr_kpis[2].metric(
            "Tỷ lệ đánh giá thấp (1–2 sao)",
            f"{month_negative_count / month_rated_count * 100:.1f}%"
            if month_rated_count else "0.0%"
        )
        mr1, mr2 = st.columns(2)
        plotly_chart(
            mr1,
            px.pie(
                month_rating_counts,
                names="review_score",
                values="orders",
                hole=0.45,
                title="Tỷ trọng đánh giá theo số sao",
                labels={"review_score": "Điểm đánh giá", "orders": "Số đơn"}
            ),
            use_container_width=True
        )
        plotly_chart(
            mr2,
            px.bar(
                month_rating_counts,
                x="review_score",
                y="orders",
                text="orders",
                title="So sánh số lượng đơn theo điểm đánh giá",
                labels={
                    "review_score": "Điểm đánh giá (sao)",
                    "orders": "Số đơn hàng"
                }
            ),
            use_container_width=True
        )

        st.markdown("#### Các đánh giá thấp cần chú ý")
        month_bad_reviews = month_reviews[
            month_reviews["review_score"] <= 2
        ].copy() if not month_reviews.empty else pd.DataFrame()
        if not month_bad_reviews.empty:
            review_title_col = next(
                (c for c in [
                    "review_comment_title", "comment_title", "review_title"
                ] if c in month_bad_reviews.columns),
                None
            )
            review_message_col = next(
                (c for c in [
                    "review_comment_message", "comment_message",
                    "review_comment", "comment"
                ] if c in month_bad_reviews.columns),
                None
            )
            review_display = pd.DataFrame({
                "Mã đơn hàng": month_bad_reviews["order_id"].astype(str),
                "Điểm đánh giá": month_bad_reviews["review_score"]
            })
            if review_title_col:
                review_display["Tiêu đề đánh giá"] = (
                    month_bad_reviews[review_title_col].fillna("").astype(str)
                )
            if review_message_col:
                review_display["Nội dung đánh giá"] = (
                    month_bad_reviews[review_message_col].fillna("").astype(str)
                )
            if review_title_col or review_message_col:
                comment_cols = [
                    c for c in [review_title_col, review_message_col] if c
                ]
                has_comment = month_bad_reviews[comment_cols].fillna("").astype(
                    str
                ).apply(
                    lambda row: any(value.strip() for value in row),
                    axis=1
                )
                review_display = review_display.loc[has_comment.values].copy()
            if not review_display.empty:
                st.dataframe(
                    review_display, use_container_width=True, hide_index=True
                )
            else:
                st.info(
                    "Có đánh giá 1–2 sao nhưng không có nội dung bình luận "
                    "để xác định nguyên nhân."
                )
        else:
            st.success("Không có đánh giá 1–2 sao trong tháng này.")

elif page == "Tình hình kinh doanh ngày":
    st.subheader("Tình hình kinh doanh ngày")
    black_caption(
        st,
        "Báo cáo sử dụng dữ liệu của ngày được chọn trong toàn bộ lịch sử của shop, "
        "không phụ thuộc bộ lọc khoảng thời gian ở thanh bên. Các ngày không có đơn "
        "vẫn được phép lựa chọn và các chỉ số được hiển thị bằng 0."
    )

    daily_source = shop_all.dropna(
        subset=["order_purchase_timestamp"]
    ).copy()
    global_dates_daily = all_sales_detail["order_purchase_timestamp"].dropna()

    if global_dates_daily.empty:
        st.info("Không có ngày hợp lệ trong dữ liệu để lập báo cáo.")
    else:
        global_min_day = global_dates_daily.min().date()
        global_max_day = global_dates_daily.max().date()
        years_daily = list(range(global_min_day.year, global_max_day.year + 1))

        # Mặc định ngày mới nhất trong phạm vi dữ liệu toàn hệ thống.
        daily_left, daily_mid, daily_right = st.columns(3)
        with daily_left:
            selected_daily_year = st.selectbox(
                "Chọn năm", years_daily,
                index=years_daily.index(global_max_day.year),
                key="daily_report_year"
            )

        months_daily = list(range(1, 13))
        if selected_daily_year == global_min_day.year and selected_daily_year == global_max_day.year:
            months_daily = list(range(global_min_day.month, global_max_day.month + 1))
        elif selected_daily_year == global_min_day.year:
            months_daily = list(range(global_min_day.month, 13))
        elif selected_daily_year == global_max_day.year:
            months_daily = list(range(1, global_max_day.month + 1))

        with daily_mid:
            selected_daily_month = st.selectbox(
                "Chọn tháng", months_daily,
                index=months_daily.index(
                    global_max_day.month if selected_daily_year == global_max_day.year
                    else months_daily[-1]
                ),
                format_func=lambda value: f"Tháng {value}",
                key="daily_report_month"
            )

        month_first_day = pd.Timestamp(
            year=int(selected_daily_year), month=int(selected_daily_month), day=1
        ).date()
        month_last_day = (
            pd.Timestamp(month_first_day) + pd.offsets.MonthEnd(0)
        ).date()
        selectable_start = max(month_first_day, global_min_day)
        selectable_end = min(month_last_day, global_max_day)
        days_daily = [
            day for day in pd.date_range(selectable_start, selectable_end, freq="D").date
        ]
        default_daily_day = (
            global_max_day if selected_daily_year == global_max_day.year
            and selected_daily_month == global_max_day.month else days_daily[-1]
        )
        with daily_right:
            selected_daily_day = st.selectbox(
                "Chọn ngày", days_daily,
                index=days_daily.index(default_daily_day),
                format_func=lambda value: value.strftime("%d/%m/%Y"),
                key="daily_report_day"
            )

        selected_date_ts = pd.Timestamp(selected_daily_day)
        previous_date = selected_date_ts - pd.Timedelta(days=1)
        daily_data = daily_source[
            daily_source["order_purchase_timestamp"].dt.date == selected_daily_day
        ].copy()
        previous_daily_data = daily_source[
            daily_source["order_purchase_timestamp"].dt.date == previous_date.date()
        ].copy()

        if daily_data.empty:
            st.info(
                f"Shop không phát sinh đơn hàng ngày {selected_daily_day:%d/%m/%Y}. "
                "Doanh thu, số đơn, sản phẩm và khách hàng được tính bằng 0; "
                "các biểu đồ theo giờ vẫn hiển thị đủ 24 giờ với giá trị 0."
            )

        day_revenue = float(daily_data["revenue"].sum())
        day_order_count = int(daily_data["order_id"].nunique())
        day_units = int(daily_data["item_key"].nunique())
        day_customers = int(daily_data["customer_unique_id"].nunique())
        day_aov = day_revenue / day_order_count if day_order_count else 0.0
        day_avg_item_price = (
            float(daily_data["price"].mean())
            if daily_data["price"].notna().any() else 0.0
        )

        day_order_ids = daily_data["order_id"].dropna().unique()
        day_orders = orders[
            orders["order_id"].isin(day_order_ids)
        ].drop_duplicates("order_id").copy()
        day_delivered = day_orders[day_orders["is_delivered"] == 1]
        all_day_ids_for_cancel = shop_all_status.loc[
            shop_all_status["order_purchase_timestamp"].dt.date == selected_daily_day,
            "order_id"
        ].dropna().unique()
        all_day_orders_for_cancel = order_status_source[
            order_status_source["order_id"].isin(all_day_ids_for_cancel)
        ].drop_duplicates("order_id")
        day_canceled = all_day_orders_for_cancel[all_day_orders_for_cancel["is_canceled"] == 1]
        day_late = day_delivered[
            pd.to_numeric(day_delivered["delivery_delay_days"], errors="coerce") > 0
        ]
        day_cancel_rate = len(day_canceled) / len(all_day_orders_for_cancel) * 100 if len(all_day_orders_for_cancel) else 0.0
        day_late_rate = len(day_late) / len(day_delivered) * 100 if len(day_delivered) else 0.0
        day_avg_delivery = (
            pd.to_numeric(day_delivered["delivery_days"], errors="coerce").mean()
            if not day_delivered.empty else 0.0
        )
        day_avg_delay = (
            pd.to_numeric(day_delivered["delivery_delay_days"], errors="coerce").mean()
            if not day_delivered.empty else 0.0
        )

        day_review_data = daily_data.drop_duplicates("order_id").copy()
        if "review_score" in day_review_data.columns:
            day_review_data["review_score"] = pd.to_numeric(
                day_review_data["review_score"], errors="coerce"
            )
            day_review_data = day_review_data.dropna(subset=["review_score"])
        else:
            day_review_data = pd.DataFrame()
        day_avg_review = (
            float(day_review_data["review_score"].mean())
            if not day_review_data.empty else 0.0
        )

        st.markdown(f"### Báo cáo ngày {selected_daily_day:%d/%m/%Y}")
        day_kpi1 = st.columns(4)
        day_kpi1[0].metric("Doanh thu hàng hóa", money(day_revenue))
        day_kpi1[1].metric("Số đơn hàng", f"{day_order_count:,}")
        day_kpi1[2].metric("Số sản phẩm đã bán", f"{day_units:,}")
        day_kpi1[3].metric("Khách hàng", f"{day_customers:,}")
        day_kpi2 = st.columns(4)
        day_kpi2[0].metric("Giá trị trung bình/đơn", money(day_aov))
        day_kpi2[1].metric("Tỷ lệ hủy đơn", f"{day_cancel_rate:.1f}%")
        day_kpi2[2].metric("Tỷ lệ giao trễ", f"{day_late_rate:.1f}%")
        day_kpi2[3].metric("Điểm đánh giá trung bình", f"{day_avg_review:.2f}/5")
        black_caption(
            st,
            "Doanh thu là tổng giá trị price của sản phẩm; không cộng phí vận chuyển. "
            "Chỉ số giao hàng được tính trên các đơn thuộc ngày đặt hàng được chọn."
        )

        # So sánh với ngày liền trước; nếu ngày trước không có đơn thì lấy giá trị 0.
        previous_order_ids = previous_daily_data["order_id"].dropna().unique()
        previous_day_orders = orders[
            orders["order_id"].isin(previous_order_ids)
        ].drop_duplicates("order_id").copy()
        previous_revenue = float(previous_daily_data["revenue"].sum())
        previous_order_count = int(previous_daily_data["order_id"].nunique())
        previous_units_count = int(previous_daily_data["item_key"].nunique())
        previous_customers_count = int(previous_daily_data["customer_unique_id"].nunique())
        previous_aov_daily = previous_revenue / previous_order_count if previous_order_count else 0.0

        def daily_delta(current, previous):
            return (current - previous) / abs(previous) * 100 if previous != 0 else None

        st.divider()
        st.markdown("### So sánh với ngày trước")
        black_caption(
            st,
            f"So sánh ngày {selected_daily_day:%d/%m/%Y} với ngày {previous_date:%d/%m/%Y}. "
            "Nếu ngày trước không phát sinh đơn, giá trị kỳ trước được tính bằng 0 "
            "và phần trăm thay đổi được ghi là chưa có cơ sở so sánh."
        )
        daily_compare_cols = st.columns(5)
        daily_compare_metrics = [
            ("Doanh thu", day_revenue, previous_revenue, money),
            ("Số đơn hàng", day_order_count, previous_order_count, lambda x: f"{x:,.0f}"),
            ("Số lượng sản phẩm", day_units, previous_units_count, lambda x: f"{x:,.0f}"),
            ("Khách hàng", day_customers, previous_customers_count, lambda x: f"{x:,.0f}"),
            ("Giá trị TB/đơn", day_aov, previous_aov_daily, money)
        ]
        for col, (label, current, previous, formatter) in zip(daily_compare_cols, daily_compare_metrics):
            delta = daily_delta(current, previous)
            col.metric(
                label, formatter(current),
                f"{delta:+.1f}% so với ngày trước" if delta is not None
                else "Ngày trước đó không phát sinh đơn hàng"
            )

        st.divider()
        st.markdown("### Diễn biến đơn hàng trong ngày")
        hourly_daily = day_orders.dropna(
            subset=["order_purchase_timestamp"]
        ).copy() if not day_orders.empty else pd.DataFrame()
        if not hourly_daily.empty:
            hourly_daily["purchase_hour"] = hourly_daily["order_purchase_timestamp"].dt.hour
            hourly_summary = (
                hourly_daily.groupby("purchase_hour")["order_id"].nunique()
                .reindex(range(24), fill_value=0).rename("orders").reset_index()
            )
        else:
            hourly_summary = pd.DataFrame({"purchase_hour": range(24), "orders": [0] * 24})
        hourly_plot = st.columns([3, 1])
        plotly_chart(
            hourly_plot[0],
            px.bar(
                hourly_summary, x="purchase_hour", y="orders", text="orders",
                title="Số đơn hàng theo từng giờ",
                labels={"purchase_hour": "Giờ trong ngày", "orders": "Số đơn hàng"}
            ), use_container_width=True
        )
        peak_hour_daily = hourly_summary.loc[hourly_summary["orders"].idxmax()]
        hourly_plot[1].metric(
            "Khung giờ cao điểm",
            f"{int(peak_hour_daily['purchase_hour']):02d}:00–"
            f"{(int(peak_hour_daily['purchase_hour']) + 1) % 24:02d}:00",
            f"{int(peak_hour_daily['orders']):,} đơn"
        )

        st.divider()
        st.markdown("### Cơ cấu sản phẩm và doanh thu trong ngày")
        day_category = (
            daily_data.groupby("product_category_name_english", as_index=False)
            .agg(
                doanh_thu=("revenue", "sum"),
                so_luong=("item_key", "nunique"),
                so_don=("order_id", "nunique")
            ).sort_values("doanh_thu", ascending=False)
            .rename(columns={
                "product_category_name_english": "Danh mục",
                "doanh_thu": "Doanh thu (BRL)",
                "so_luong": "Số lượng bán", "so_don": "Số đơn hàng"
            })
        )
        if not day_category.empty:
            day_cat_cols = st.columns(2)
            plotly_chart(
                day_cat_cols[0],
                px.bar(
                    day_category.head(10).sort_values("Doanh thu (BRL)"),
                    x="Doanh thu (BRL)", y="Danh mục", orientation="h",
                    title="Top danh mục theo doanh thu"
                ), use_container_width=True
            )
            plotly_chart(
                day_cat_cols[1],
                px.bar(
                    day_category.head(10).sort_values("Số lượng bán"),
                    x="Số lượng bán", y="Danh mục", orientation="h",
                    title="Top danh mục theo số lượng bán"
                ), use_container_width=True
            )
            st.dataframe(day_category, use_container_width=True, hide_index=True)
        else:
            st.info("Không có dữ liệu danh mục sản phẩm trong ngày này; doanh thu và số lượng đều bằng 0.")

        day_products = (
            daily_data.groupby(["product_id", "product_category_name_english"], as_index=False, dropna=False)
            .agg(
                so_luong=("item_key", "nunique"),
                so_don=("order_id", "nunique"),
                gia_trung_binh=("price", "mean"),
                doanh_thu=("revenue", "sum")
            ).sort_values("doanh_thu", ascending=False)
        )
        st.markdown("#### Bảng sản phẩm bán trong ngày")
        if not day_products.empty:
            day_products_display = day_products.rename(columns={
                "product_id": "Mã sản phẩm", "product_category_name_english": "Danh mục sản phẩm", "so_luong": "Số lượng bán",
                "so_don": "Số đơn hàng", "gia_trung_binh": "Giá sản phẩm trung bình (BRL)",
                "doanh_thu": "Doanh thu (BRL)"
            })
            st.dataframe(
                day_products_display, use_container_width=True, hide_index=True,
                column_config={
                    "Giá sản phẩm trung bình (BRL)": st.column_config.NumberColumn(format="R$ %.2f"),
                    "Doanh thu (BRL)": st.column_config.NumberColumn(format="R$ %.2f")
                }
            )
        else:
            st.info("Không có sản phẩm được bán trong ngày được chọn.")

        st.divider()
        st.markdown("### Trạng thái đơn hàng và hiệu quả giao hàng")
        status_day_order_ids = shop_all_status.loc[
            shop_all_status["order_purchase_timestamp"].dt.date == selected_daily_day,
            "order_id"
        ].dropna().unique()
        status_day_orders = order_status_source[
            order_status_source["order_id"].isin(status_day_order_ids)
        ].drop_duplicates("order_id")
        status_daily = (
            status_day_orders["order_status"].fillna("Không xác định").value_counts()
            .rename_axis("Trạng thái").reset_index(name="Số đơn")
        ) if not status_day_orders.empty else pd.DataFrame({"Trạng thái": [], "Số đơn": []})
        status_cols = st.columns(2)
        if not status_daily.empty:
            plotly_chart(
                status_cols[0],
                px.pie(status_daily, names="Trạng thái", values="Số đơn", hole=0.45,
                       title="Cơ cấu trạng thái đơn hàng"), use_container_width=True
            )
        else:
            status_cols[0].info("Không có đơn hàng để thống kê trạng thái trong ngày.")
        daily_delivery_table = pd.DataFrame({
            "Chỉ tiêu": [
                "Số đơn đã giao thành công", "Số đơn bị hủy", "Số đơn giao trễ",
                "Thời gian giao hàng trung bình (ngày)", "Số ngày trễ trung bình (ngày)"
            ],
            "Giá trị": [
                len(day_delivered), len(day_canceled), len(day_late),
                round(day_avg_delivery, 2) if pd.notna(day_avg_delivery) else 0.0,
                round(day_avg_delay, 2) if pd.notna(day_avg_delay) else 0.0
            ]
        })
        status_cols[1].dataframe(daily_delivery_table, use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("### Phân tích khách hàng và khu vực trong ngày")
        daily_geo = (
            daily_data.dropna(subset=["customer_state"])
            .groupby("customer_state", as_index=False)
            .agg(
                revenue=("revenue", "sum"), orders=("order_id", "nunique"),
                customers=("customer_unique_id", "nunique")
            ).sort_values("revenue", ascending=False)
        ) if "customer_state" in daily_data.columns else pd.DataFrame()
        if not daily_geo.empty:
            daily_geo_cols = st.columns(2)
            plotly_chart(
                daily_geo_cols[0],
                px.bar(
                    daily_geo.head(10).sort_values("revenue"), x="revenue",
                    y="customer_state", orientation="h", title="Doanh thu theo khu vực",
                    labels={"revenue": "Doanh thu (BRL)", "customer_state": "Bang khách hàng"}
                ), use_container_width=True
            )
            plotly_chart(
                daily_geo_cols[1],
                px.bar(
                    daily_geo.head(10).sort_values("orders"), x="orders",
                    y="customer_state", orientation="h", title="Số đơn hàng theo khu vực",
                    labels={"orders": "Số đơn hàng", "customer_state": "Bang khách hàng"}
                ), use_container_width=True
            )
            st.dataframe(
                daily_geo.rename(columns={
                    "customer_state": "Khu vực / Bang", "revenue": "Doanh thu (BRL)",
                    "orders": "Số đơn hàng", "customers": "Số khách hàng"
                }), use_container_width=True, hide_index=True
            )
        else:
            st.info("Không có thông tin khu vực khách hàng trong ngày này.")

        st.divider()
        st.markdown("### Đánh giá của khách hàng trong ngày")
        if not day_review_data.empty:
            daily_rating_counts = (
                day_review_data.groupby("review_score")["order_id"].nunique()
                .reindex([1, 2, 3, 4, 5], fill_value=0)
                .rename("orders").reset_index()
            )
            daily_negative_count = day_review_data.loc[
                day_review_data["review_score"] <= 2, "order_id"
            ].nunique()
        else:
            daily_rating_counts = pd.DataFrame({"review_score": [1, 2, 3, 4, 5], "orders": [0] * 5})
            daily_negative_count = 0
        review_daily_cols = st.columns(3)
        review_daily_cols[0].metric("Điểm đánh giá trung bình", f"{day_avg_review:.2f}/5")
        review_daily_cols[1].metric("Đơn được đánh giá", f"{len(day_review_data):,}")
        review_daily_cols[2].metric(
            "Tỷ lệ đánh giá thấp (1–2 sao)",
            f"{daily_negative_count / len(day_review_data) * 100:.1f}%"
            if len(day_review_data) else "0.0%"
        )
        review_chart_cols = st.columns(2)
        plotly_chart(
            review_chart_cols[0],
            px.pie(
                daily_rating_counts, names="review_score", values="orders", hole=0.45,
                title="Tỷ trọng đánh giá theo số sao",
                labels={"review_score": "Điểm đánh giá", "orders": "Số đơn"}
            ), use_container_width=True
        )
        plotly_chart(
            review_chart_cols[1],
            px.bar(
                daily_rating_counts, x="review_score", y="orders", text="orders",
                title="Số đơn theo điểm đánh giá",
                labels={"review_score": "Điểm đánh giá (sao)", "orders": "Số đơn hàng"}
            ), use_container_width=True
        )
        if day_review_data.empty:
            st.info("Chưa có đánh giá của khách hàng cho các đơn hàng trong ngày này.")

elif page == "Khách hàng và địa lý":
    st.subheader("Khách hàng, hành vi mua sắm và phân bố địa lý")
    black_caption(
        st,
        "Phân tích số lần đặt hàng, chi tiêu, khách hàng mua lại, "
        "phân khúc RFM và khả năng giữ chân khách hàng."
    )

    # Tạo dữ liệu ở cấp đơn hàng trước khi tính chỉ số khách hàng.
    # f đang ở cấp sản phẩm-đơn hàng, vì vậy phải groupby order_id
    # để không đếm một đơn nhiều lần khi đơn có nhiều sản phẩm.
    customer_order_level = (
        f.groupby(
            [
                "order_id", "customer_unique_id", "customer_state",
                "order_purchase_timestamp"
            ],
            as_index=False,
            dropna=False
        )
        .agg(
            order_spend=("revenue", "sum"),
            is_delivered=("is_delivered", "max"),
            is_canceled=("is_canceled", "max")
        )
    )
    customer_order_level["purchase_month"] = (
        customer_order_level["order_purchase_timestamp"]
        .dt.to_period("M")
    )
    customer_order_level = customer_order_level.dropna(
        subset=["customer_unique_id", "order_purchase_timestamp"]
    )

    # Bộ lọc giao thành công bắt buộc trên toàn bộ dashboard.
    customer_base = customer_order_level.copy()
    black_caption(st, "Chỉ thống kê khách hàng của các đơn đã giao thành công.")

    if customer_base.empty:
        st.info("Không phát sinh đơn hàng trong khoảng thời gian đã chọn. Các chỉ số khách hàng bằng 0.")
        z = st.columns(5)
        z[0].metric("Khách hàng", "0")
        z[1].metric("Tổng đơn hàng", "0")
        z[2].metric("Đơn TB/khách", "0.00")
        z[3].metric("Chi tiêu TB/khách", money(0))
        z[4].metric("Tỷ lệ mua lại", "0.00%")
        st.markdown("### Phân tích khách hàng theo địa lý")
        st.metric("Số khu vực có khách hàng", "0")
    else:
        # Một khách hàng có thể có nhiều customer_key, nên định danh
        # khách hàng bằng customer_unique_id.
        customer_summary = (
            customer_base.groupby("customer_unique_id", as_index=False)
            .agg(
                order_count=("order_id", "nunique"),
                total_spent=("order_spend", "sum"),
                first_order=("order_purchase_timestamp", "min"),
                last_order=("order_purchase_timestamp", "max"),
                average_order_value=("order_spend", "mean"),
                state=("customer_state", "last")
            )
        )
        customer_summary["first_order"] = pd.to_datetime(
            customer_summary["first_order"], errors="coerce"
        )
        customer_summary["last_order"] = pd.to_datetime(
            customer_summary["last_order"], errors="coerce"
        )

        total_customers = customer_summary["customer_unique_id"].nunique()
        total_customer_orders = customer_base["order_id"].nunique()
        total_customer_spend = customer_summary["total_spent"].sum()
        repeat_customers = int((customer_summary["order_count"] >= 2).sum())
        repeat_rate = (
            repeat_customers / total_customers * 100
            if total_customers else 0
        )
        avg_orders_per_customer = (
            customer_summary["order_count"].mean()
            if total_customers else 0
        )
        avg_spend_per_customer = (
            total_customer_spend / total_customers
            if total_customers else 0
        )
        customer_aov = (
            total_customer_spend / total_customer_orders
            if total_customer_orders else 0
        )

        st.markdown("### Tổng quan khách hàng")
        kpis = st.columns(5)
        kpis[0].metric("Khách hàng", f"{total_customers:,}")
        kpis[1].metric("Tổng đơn hàng", f"{total_customer_orders:,}")
        kpis[2].metric("Đơn TB/khách", f"{avg_orders_per_customer:.2f}")
        kpis[3].metric("Chi tiêu TB/khách", money(avg_spend_per_customer))
        kpis[4].metric("Tỷ lệ mua lại", f"{repeat_rate:.2f}%")
        black_caption(
            st,
            f"Tổng chi tiêu: {money(total_customer_spend)} · "
            f"Giá trị trung bình mỗi đơn (AOV): {money(customer_aov)}. "
            "Doanh thu/chi tiêu được tính bằng tổng giá trị price của sản phẩm."
        )

        st.divider()
        st.markdown("### 1. Tần suất đặt hàng và chi tiêu")

        # Khách mua lần đầu tại shop so với khách đã từng mua trước kỳ lọc.
        # Sử dụng lịch sử đơn giao thành công của shop, không chỉ các đơn trong kỳ.
        first_delivered_purchase = (
            shop_all.dropna(subset=["customer_unique_id", "order_purchase_timestamp"])
            .groupby("customer_unique_id")["order_purchase_timestamp"]
            .min()
        )
        first_purchase_in_period = customer_summary["customer_unique_id"].map(
            first_delivered_purchase
        )
        first_time_count = int((first_purchase_in_period.dt.date >= start).sum())
        existing_count = int(total_customers - first_time_count)
        customer_origin = pd.DataFrame({
            "Nhóm khách hàng": ["Khách mua lần đầu", "Khách hàng cũ"],
            "Số khách hàng": [first_time_count, existing_count]
        })
        plotly_chart(
            st,
            px.pie(
                customer_origin,
                names="Nhóm khách hàng",
                values="Số khách hàng",
                hole=0.5,
                title="Tỷ lệ khách mua lần đầu và khách hàng cũ"
            ),
            use_container_width=True
        )

        left, right = st.columns(2)
        frequency = (
            customer_summary["order_count"]
            .value_counts()
            .sort_index()
            .rename_axis("Số lần đặt hàng")
            .reset_index(name="Số khách hàng")
        )
        frequency["Nhóm số lần đặt"] = np.where(
            frequency["Số lần đặt hàng"] >= 6,
            "6 lần trở lên",
            frequency["Số lần đặt hàng"].astype(str) + " lần"
        )
        frequency_group = (
            customer_summary.assign(
                frequency_group=pd.cut(
                    customer_summary["order_count"],
                    bins=[0, 1, 3, 5, np.inf],
                    labels=["1 lần", "2–3 lần", "4–5 lần", "Trên 5 lần"]
                )
            )
            .groupby("frequency_group", observed=False)
            .agg(
                customers=("customer_unique_id", "nunique"),
                spending=("total_spent", "sum")
            )
            .reset_index()
        )
        plotly_chart(
            left,
            px.bar(
                frequency_group,
                x="frequency_group",
                y="customers",
                text="customers",
                title="Phân phối khách hàng theo số lần đặt đơn",
                labels={
                    "frequency_group": "Số lần đặt hàng",
                    "customers": "Số khách hàng"
                }
            ),
            use_container_width=True
        )

        spending_distribution = customer_summary.copy()
        spending_score = np.ceil(
            spending_distribution["total_spent"].rank(method="first", pct=True) * 5
        ).clip(1, 5).astype(int)
        spending_labels = {
            1: "Thấp nhất", 2: "Thấp", 3: "Trung bình",
            4: "Cao", 5: "Cao nhất"
        }
        spending_distribution["Nhóm chi tiêu"] = spending_score.map(
            spending_labels
        )
        spending_group = (
            spending_distribution.groupby("Nhóm chi tiêu", observed=False)
            .agg(
                customers=("customer_unique_id", "nunique"),
                spending=("total_spent", "sum")
            )
            .reset_index()
        )
        plotly_chart(
            right,
            px.bar(
                spending_group,
                x="Nhóm chi tiêu",
                y="spending",
                text="customers",
                title="Tổng chi tiêu theo 5 nhóm khách hàng",
                labels={
                    "spending": "Tổng chi tiêu (BRL)",
                    "customers": "Số khách hàng"
                }
            ),
            use_container_width=True
        )

        repeat_left, repeat_right = st.columns(2)
        repeat_breakdown = (
            customer_summary.assign(
                purchase_type=np.where(
                    customer_summary["order_count"] >= 2,
                    "Khách mua lại",
                    "Chỉ mua một lần"
                )
            )
            .groupby("purchase_type", as_index=False)
            .agg(
                customers=("customer_unique_id", "nunique"),
                spending=("total_spent", "sum")
            )
        )
        plotly_chart(
            repeat_left,
            px.pie(
                repeat_breakdown,
                names="purchase_type",
                values="customers",
                hole=0.5,
                title="Tỷ trọng khách mua một lần và mua lại"
            ),
            use_container_width=True
        )
        plotly_chart(
            repeat_right,
            px.bar(
                repeat_breakdown,
                x="purchase_type",
                y="spending",
                title="Chi tiêu theo nhóm khách hàng",
                labels={
                    "purchase_type": "Nhóm khách hàng",
                    "spending": "Tổng chi tiêu (BRL)"
                }
            ),
            use_container_width=True
        )

        st.divider()
        st.markdown("### 2. Khách hàng chi tiêu nhiều nhất")
        top_n = st.selectbox(
            "Số lượng khách hàng hiển thị",
            [10, 20, 50, 100],
            index=1,
            key="customer_top_n"
        )
        top_customers = customer_summary.nlargest(
            top_n, "total_spent"
        ).copy()
        top_customers["total_spent_display"] = top_customers[
            "total_spent"
        ].map(money)
        top_customers["average_order_value_display"] = top_customers[
            "average_order_value"
        ].map(money)
        st.dataframe(
            top_customers[
                [
                    "customer_unique_id", "state", "order_count",
                    "total_spent_display", "average_order_value_display",
                    "first_order", "last_order"
                ]
            ].rename(columns={
                "customer_unique_id": "Mã khách hàng",
                "state": "Bang",
                "order_count": "Số đơn",
                "total_spent_display": "Tổng chi tiêu",
                "average_order_value_display": "Chi tiêu TB/đơn",
                "first_order": "Đơn đầu tiên",
                "last_order": "Đơn gần nhất"
            }),
            use_container_width=True,
            hide_index=True
        )

        st.divider()
        st.markdown("### 3. Phân khúc khách hàng RFM")
        black_caption(
            st,
            "RFM được tính trên tập đơn hàng đang lọc: Recency là số ngày "
            "từ lần mua gần nhất đến ngày cuối của dữ liệu đang xét; "
            "Frequency là số đơn; Monetary là tổng chi tiêu."
        )

        reference_date = (
            customer_base["order_purchase_timestamp"].max().normalize()
            + pd.Timedelta(days=1)
        )
        rfm = customer_summary[
            ["customer_unique_id", "order_count", "total_spent", "last_order"]
        ].copy()
        rfm["recency_days"] = (
            reference_date - rfm["last_order"]
        ).dt.days.clip(lower=0)

        # Rank trước khi qcut để tránh lỗi khi nhiều khách hàng có cùng giá trị.
        # Dùng percentile rank thay qcut để vẫn chạy được khi tập khách hàng
        # nhỏ hoặc có nhiều giá trị trùng nhau.
        def calculate_rfm_score(series, reverse=False):
            if series.nunique(dropna=True) <= 1:
                return pd.Series(3, index=series.index, dtype=int)

            percentile = series.rank(
                method="average",
                pct=True,
                ascending=not reverse
            )

            score = np.ceil(percentile * 5).clip(1, 5)

            return score.astype(int)


        rfm["R_score"] = calculate_rfm_score(
            rfm["recency_days"],
            reverse=True
        )

        rfm["F_score"] = calculate_rfm_score(
            rfm["order_count"]
        )

        rfm["M_score"] = calculate_rfm_score(
            rfm["total_spent"]
        )


        # Phân loại khách hàng theo RFM
        def classify_rfm(row):
            r = row["R_score"]
            freq = row["F_score"]
            m = row["M_score"]

            # Khách hàng giá trị nhất
            if r >= 4 and freq >= 4 and m >= 4:
                return "Khách hàng giá trị nhất"

            # Khách hàng có nguy cơ rời bỏ
            if r <= 2 and freq >= 3:
                return "Khách hàng có nguy cơ rời bỏ"

            # Khách hàng đã rời bỏ
            if r <= 2 and freq <= 2:
                return "Khách hàng đã rời bỏ"

            # Khách hàng trung thành
            if r >= 3 and freq >= 4 and row["order_count"] >= 3:
                return "Khách hàng trung thành"

            # Khách hàng mới / gần đây
            if r >= 4 and freq <= 2:
                return "Khách hàng mới / gần đây"

            # Khách hàng chi tiêu cao
            if m >= 4:
                return "Khách hàng chi tiêu cao"

            return "Khách hàng tiềm năng"

        rfm["segment"] = rfm.apply(classify_rfm, axis=1)
        segment_summary = (
            rfm.groupby("segment", as_index=False)
            .agg(
                customers=("customer_unique_id", "nunique"),
                total_spent=("total_spent", "sum"),
                avg_orders=("order_count", "mean"),
                avg_recency=("recency_days", "mean")
            )
            .sort_values("total_spent", ascending=False)
        )
        seg_left, seg_right = st.columns(2)
        plotly_chart(
            seg_left,
            px.pie(
                segment_summary,
                names="segment",
                values="customers",
                hole=0.45,
                title="Số lượng khách hàng theo phân khúc RFM"
            ),
            use_container_width=True
        )
        plotly_chart(
            seg_right,
            px.bar(
                segment_summary,
                x="segment",
                y="total_spent",
                title="Chi tiêu theo phân khúc RFM",
                labels={
                    "segment": "Phân khúc khách hàng",
                    "total_spent": "Tổng chi tiêu (BRL)"
                }
            ),
            use_container_width=True
        )
        st.dataframe(
            segment_summary.rename(columns={
                "segment": "Phân khúc khách hàng",
                "customers": "Số khách hàng",
                "total_spent": "Tổng chi tiêu (BRL)",
                "avg_orders": "Số đơn trung bình/khách",
                "avg_recency": "Số ngày kể từ lần mua gần nhất (TB)"
            }),
            use_container_width=True,
            hide_index=True
        )

        segment_filter = st.multiselect(
            "Lọc phân khúc để xem danh sách khách hàng",
            sorted(rfm["segment"].unique()),
            default=sorted(rfm["segment"].unique()),
            key="rfm_segment_filter"
        )
        rfm_detail = rfm[rfm["segment"].isin(segment_filter)].merge(
            customer_summary[
                ["customer_unique_id", "state", "average_order_value"]
            ],
            on="customer_unique_id",
            how="left"
        )
        st.dataframe(
            rfm_detail.sort_values(
                ["R_score", "F_score", "M_score"],
                ascending=False
            ).rename(columns={
                "customer_unique_id": "Mã khách hàng",
                "state": "Bang",
                "order_count": "Số đơn",
                "total_spent": "Tổng chi tiêu (BRL)",
                "average_order_value": "Chi tiêu TB/đơn (BRL)",
                "recency_days": "Số ngày kể từ lần mua gần nhất",
                "R_score": "R",
                "F_score": "F",
                "M_score": "M",
                "segment": "Phân khúc khách hàng"
            }),
            use_container_width=True,
            hide_index=True
        )

        st.divider()
        st.markdown("### 4. Phân tích khách hàng theo địa lý")
        state_summary = (
            customer_base.groupby("customer_state", as_index=False)
            .agg(
                orders=("order_id", "nunique"),
                revenue=("order_spend", "sum"),
                customers=("customer_unique_id", "nunique")
            )
            .sort_values("revenue", ascending=False)
        )
        geo_base = f.dropna(subset=["latitude", "longitude"])
        geo_data = (
            geo_base.groupby(
                ["customer_state", "latitude", "longitude"],
                as_index=False
            )
            .agg(
                revenue=("revenue", "sum"),
                orders=("order_id", "nunique")
            )
        )
        geo_left, geo_right = st.columns(2)
        plotly_chart(
            geo_left,
            px.bar(
                state_summary.head(15),
                x="customer_state",
                y="revenue",
                title="Chi tiêu theo bang khách hàng",
                labels={
                    "customer_state": "Bang",
                    "revenue": "Chi tiêu (BRL)"
                }
            ),
            use_container_width=True
        )
        plotly_chart(
            geo_right,
            px.bar(
                state_summary.head(15),
                x="customer_state",
                y="customers",
                title="Số khách hàng theo bang",
                labels={
                    "customer_state": "Bang",
                    "customers": "Số khách hàng"
                }
            ),
            use_container_width=True
        )
        if not geo_data.empty:
            plotly_chart(
                st,
                px.scatter_geo(
                    geo_data,
                    lat="latitude",
                    lon="longitude",
                    size="orders",
                    color="revenue",
                    color_continuous_scale=["#EF9A9A", "#EF5350", "#C62828", "#8E151A", "#560B14"],
                    hover_name="customer_state",
                    scope="south america",
                    title="Phân bố đơn hàng và chi tiêu theo tọa độ"
                ),
                use_container_width=True
            )
        st.dataframe(
            state_summary.rename(columns={
                "customer_state": "Bang",
                "orders": "Số đơn",
                "revenue": "Tổng chi tiêu (BRL)",
                "customers": "Số khách hàng"
            }),
            use_container_width=True,
            hide_index=True
        )

        black_caption(
            st,
            "Lưu ý: Các chỉ số khách hàng trong trang này tuân theo khoảng "
            "thời gian và bộ lọc shop ở sidebar. Khi chọn một shop, tần suất "
            "và chi tiêu được tính riêng trên các đơn có sản phẩm thuộc shop đó; "
            "một đơn có nhiều shop có thể xuất hiện trong phân tích của từng shop."
        )

elif page == "Đơn hàng và vận hành":
    st.subheader("Đơn hàng và vận hành")
    # Lọc đơn ở cấp order; chỉ giữ đơn có sản phẩm thuộc shop đang chọn.
    selected_order_ids = f["order_id"].dropna().unique()
    order_view = orders[
        orders["order_id"].isin(selected_order_ids)
    ].drop_duplicates("order_id").copy()

    if order_view.empty:
        st.info(
            "Không phát sinh đơn hàng trong khoảng thời gian đã chọn. "
            "Các chỉ số, trạng thái và chuỗi thời gian được hiển thị bằng 0."
        )

    # Tạo đủ danh mục trạng thái phổ biến có trong hệ thống; trạng thái không
    # phát sinh trong kỳ vẫn xuất hiện với số lượng bằng 0.
    status_order = ["delivered", "shipped", "invoiced", "processing",
                    "created", "approved", "canceled", "unavailable"]
    status_order_ids = status_f["order_id"].dropna().unique()
    status_order_view = order_status_source[
        order_status_source["order_id"].isin(status_order_ids)
    ].drop_duplicates("order_id")
    status_counts = (
        status_order_view["order_status"].fillna("unknown").astype(str)
        .value_counts().to_dict()
    ) if not status_order_view.empty else {}
    for status_name in status_order:
        status_counts.setdefault(status_name, 0)
    status_counts.setdefault("unknown", 0)
    status_data = pd.DataFrame({
        "Trạng thái": list(status_counts.keys()),
        "Số đơn hàng": list(status_counts.values())
    })
    status_data = status_data.sort_values("Số đơn hàng", ascending=False)

    st.markdown("### Trạng thái đơn hàng")
    status_left, status_right = st.columns(2)
    plotly_chart(
        status_left,
        px.bar(
            status_data, x="Trạng thái", y="Số đơn hàng", text="Số đơn hàng",
            title="Số lượng đơn hàng theo trạng thái"
        ), use_container_width=True
    )
    plotly_chart(
        status_right,
        px.pie(
            status_data[status_data["Số đơn hàng"] > 0]
            if status_data["Số đơn hàng"].sum() > 0 else status_data,
            names="Trạng thái", values="Số đơn hàng", hole=0.45,
            title="Cơ cấu trạng thái đơn hàng"
        ), use_container_width=True
    )
    st.dataframe(status_data, use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("### Số lượng đơn hàng theo tháng")
    monthly_orders = (
        order_view.assign(month=order_view["order_purchase_timestamp"].dt.to_period("M").dt.to_timestamp())
        .groupby("month", as_index=False).agg(orders=("order_id", "nunique"))
        if not order_view.empty else pd.DataFrame(columns=["month", "orders"])
    )
    monthly_orders = complete_time_series(
        monthly_orders, "month", "orders", start, end, freq="M"
    )
    monthly_orders["month"] = monthly_orders["month"].dt.strftime("%Y-%m")
    plotly_chart(
        st,
        px.line(
            monthly_orders, x="month", y="orders", markers=True,
            title="Số đơn hàng theo tháng",
            labels={"month": "Tháng", "orders": "Số đơn hàng"}
        ), use_container_width=True
    )

    st.markdown("### Số lượng đơn hàng theo ngày")
    daily_orders = (
        order_view.assign(day=order_view["order_purchase_timestamp"].dt.normalize())
        .groupby("day", as_index=False).agg(orders=("order_id", "nunique"))
        if not order_view.empty else pd.DataFrame(columns=["day", "orders"])
    )
    daily_orders = complete_time_series(
        daily_orders, "day", "orders", start, end, freq="D"
    )
    plotly_chart(
        st,
        px.line(
            daily_orders,
            x="day",
            y="orders",
            markers=True,
            title="Số đơn hàng phát sinh theo ngày",
            labels={
                "day": "Ngày",
                "orders": "Số đơn hàng"
            }
        ),
        use_container_width=True
    )

    st.divider()
    st.markdown("### Số lượng đơn hàng theo giờ trong ngày")
    hourly = order_view.dropna(subset=["order_purchase_timestamp"]).copy()
    if not hourly.empty:
        hourly["hour"] = hourly["order_purchase_timestamp"].dt.hour
        hourly_counts = hourly.groupby("hour")["order_id"].nunique().reindex(range(24), fill_value=0)
    else:
        hourly_counts = pd.Series(0, index=range(24))
    hourly_data = hourly_counts.rename_axis("Giờ").reset_index(name="Số đơn hàng")
    h1, h2 = st.columns([3, 1])
    plotly_chart(
        h1,
        px.bar(hourly_data, x="Giờ", y="Số đơn hàng", text="Số đơn hàng",
               title="Đơn hàng phát sinh theo từng giờ"),
        use_container_width=True
    )
    peak_hour = int(hourly_data.loc[hourly_data["Số đơn hàng"].idxmax(), "Giờ"])
    peak_hour_count = int(hourly_data["Số đơn hàng"].max())
    h2.metric("Khung giờ cao điểm", f"{peak_hour:02d}:00–{(peak_hour + 1) % 24:02d}:00", f"{peak_hour_count:,} đơn")

    st.divider()
    st.markdown("### So sánh số lượng đơn hàng theo ngày trong tuần")
    weekday_labels = {0: "Thứ 2", 1: "Thứ 3", 2: "Thứ 4", 3: "Thứ 5", 4: "Thứ 6", 5: "Thứ 7", 6: "Chủ nhật"}
    if not order_view.empty:
        weekday_temp = order_view.dropna(subset=["order_purchase_timestamp"]).copy()
        weekday_temp["weekday"] = weekday_temp["order_purchase_timestamp"].dt.dayofweek
        weekday_counts = weekday_temp.groupby("weekday")["order_id"].nunique().reindex(range(7), fill_value=0)
    else:
        weekday_counts = pd.Series(0, index=range(7))
    weekday_data = weekday_counts.rename_axis("weekday").reset_index(name="Số đơn hàng")
    weekday_data["Ngày trong tuần"] = weekday_data["weekday"].map(weekday_labels)
    w1, w2 = st.columns([3, 1])
    plotly_chart(
        w1,
        px.bar(weekday_data, x="Ngày trong tuần", y="Số đơn hàng", text="Số đơn hàng",
               category_orders={"Ngày trong tuần": list(weekday_labels.values())},
               title="Số đơn hàng theo thứ trong tuần"),
        use_container_width=True
    )
    peak_weekday = weekday_data.loc[weekday_data["Số đơn hàng"].idxmax()]
    w2.metric("Ngày có nhiều đơn nhất", peak_weekday["Ngày trong tuần"], f"{int(peak_weekday['Số đơn hàng']):,} đơn")

    st.divider()
    st.markdown("### Chỉ số giao hàng và hủy đơn")
    delivered = order_view[order_view["is_delivered"] == 1].copy()
    delivered_count = int(delivered["order_id"].nunique())
    late_count = int((pd.to_numeric(delivered["delivery_delay_days"], errors="coerce").fillna(0) > 0).sum())
    cancel_rate = float(pd.to_numeric(status_order_view["is_canceled"], errors="coerce").fillna(0).mean() * 100) if not status_order_view.empty else 0.0
    late_rate = late_count / delivered_count * 100 if delivered_count else 0.0
    avg_delivery = pd.to_numeric(delivered["delivery_days"], errors="coerce").mean() if delivered_count else 0.0
    ops_kpis = st.columns(4)
    ops_kpis[0].metric("Tỷ lệ hủy", f"{cancel_rate:.1f}%")
    ops_kpis[1].metric("Thời gian giao TB", f"{avg_delivery:.1f} ngày" if pd.notna(avg_delivery) else "0.0 ngày")
    ops_kpis[2].metric("Số đơn giao trễ", f"{late_count:,}")
    ops_kpis[3].metric("Tỷ lệ giao trễ", f"{late_rate:.1f}%")

    delay_monthly = (
        delivered.assign(month=delivered["order_purchase_timestamp"].dt.to_period("M").dt.to_timestamp())
        .groupby("month", as_index=False).agg(delivery_delay_days=("delivery_delay_days", "mean"))
        if not delivered.empty else pd.DataFrame(columns=["month", "delivery_delay_days"])
    )
    delay_monthly = complete_time_series(delay_monthly, "month", "delivery_delay_days", start, end, freq="M")
    delay_monthly["month"] = delay_monthly["month"].dt.strftime("%Y-%m")
    plotly_chart(st, px.line(delay_monthly, x="month", y="delivery_delay_days", markers=True,
                            title="Độ trễ giao hàng trung bình theo tháng",
                            labels={"month": "Tháng", "delivery_delay_days": "Số ngày trễ TB"}),
                    use_container_width=True)

    # Phân tích độ trễ của các đơn giao trễ theo khu vực khách hàng và danh mục.
    # Mỗi đơn hàng chỉ được tính một lần trong từng khu vực/danh mục.
    st.markdown("#### Thời gian giao hàng trễ trung bình theo khu vực đặt hàng")
    delay_order_ids = delivered.loc[
        pd.to_numeric(delivered["delivery_delay_days"], errors="coerce") > 0,
        "order_id"
    ].dropna().astype(str).unique()

    shop_items_for_delay = d[
        (d["seller_id"].astype(str) == selected_shop)
        & d["order_id"].astype(str).isin(delay_order_ids)
    ].copy()

    delay_region_base = shop_items_for_delay[
        ["order_id", "customer_state"]
    ].dropna(subset=["customer_state"]).drop_duplicates("order_id")
    delay_values = delivered[
        delivered["order_id"].astype(str).isin(delay_order_ids)
    ][["order_id", "delivery_delay_days"]].drop_duplicates("order_id")
    delay_region = delay_region_base.merge(
        delay_values, on="order_id", how="inner"
    )
    if not delay_region.empty:
        delay_region_chart = (
            delay_region.groupby("customer_state", as_index=False)
            .agg(
                average_delay_days=("delivery_delay_days", "mean"),
                late_orders=("order_id", "nunique")
            )
            .sort_values("average_delay_days", ascending=True)
        )
        plotly_chart(
            st,
            px.bar(
                delay_region_chart,
                x="average_delay_days",
                y="customer_state",
                orientation="h",
                text="average_delay_days",
                hover_data={"late_orders": True},
                title="Số ngày giao trễ trung bình theo khu vực đặt hàng",
                labels={
                    "customer_state": "Bang / Khu vực đặt hàng",
                    "average_delay_days": "Số ngày trễ trung bình",
                    "late_orders": "Số đơn giao trễ"
                }
            ),
            use_container_width=True
        )
    else:
        st.info("Không có đơn giao trễ trong kỳ; thời gian giao trễ trung bình theo khu vực là 0 ngày.")

    st.markdown("#### Thời gian giao hàng trễ trung bình theo danh mục sản phẩm")
    delay_category_base = shop_items_for_delay[
        ["order_id", "product_category_name_english"]
    ].dropna(subset=["product_category_name_english"]).drop_duplicates(
        ["order_id", "product_category_name_english"]
    )
    delay_category = delay_category_base.merge(
        delay_values, on="order_id", how="inner"
    )
    if not delay_category.empty:
        delay_category_chart = (
            delay_category.groupby(
                "product_category_name_english", as_index=False
            )
            .agg(
                average_delay_days=("delivery_delay_days", "mean"),
                late_orders=("order_id", "nunique")
            )
            .sort_values("average_delay_days", ascending=True)
        )
        plotly_chart(
            st,
            px.bar(
                delay_category_chart,
                x="average_delay_days",
                y="product_category_name_english",
                orientation="h",
                text="average_delay_days",
                hover_data={"late_orders": True},
                title="Số ngày giao trễ trung bình theo danh mục sản phẩm",
                labels={
                    "product_category_name_english": "Danh mục sản phẩm",
                    "average_delay_days": "Số ngày trễ trung bình",
                    "late_orders": "Số đơn giao trễ"
                }
            ),
            use_container_width=True
        )
    else:
        st.info("Không có đơn giao trễ trong kỳ; thời gian giao trễ trung bình theo danh mục là 0 ngày.")

    st.divider()
    st.markdown("### Đánh giá của khách hàng")
    black_caption(st, "Đánh giá được tính theo đơn hàng thuộc shop đang chọn và khoảng thời gian đang lọc. Mỗi đơn chỉ được tính một lần.")
    if reviews is not None and not reviews.empty and "order_id" in reviews.columns:
        selected_reviews = reviews[reviews["order_id"].astype(str).isin(order_view["order_id"].astype(str))].copy()
    else:
        selected_reviews = pd.DataFrame(columns=["order_id", "review_score"])
    if "review_score" in selected_reviews.columns:
        selected_reviews["review_score"] = pd.to_numeric(selected_reviews["review_score"], errors="coerce")
        selected_reviews = selected_reviews.dropna(subset=["review_score"]).drop_duplicates("order_id", keep="last")
    else:
        selected_reviews = pd.DataFrame(columns=["order_id", "review_score"])

    rating_counts = selected_reviews.groupby("review_score")["order_id"].nunique().reindex([1, 2, 3, 4, 5], fill_value=0) if not selected_reviews.empty else pd.Series(0, index=[1, 2, 3, 4, 5])
    rating_data = rating_counts.rename_axis("Điểm đánh giá").reset_index(name="Số đơn hàng")
    rated_count = int(selected_reviews["order_id"].nunique()) if not selected_reviews.empty else 0
    avg_score = float(selected_reviews["review_score"].mean()) if rated_count else 0.0
    negative_count = int(selected_reviews.loc[selected_reviews["review_score"] <= 2, "order_id"].nunique()) if rated_count else 0
    rk = st.columns(3)
    rk[0].metric("Điểm đánh giá trung bình", f"{avg_score:.2f}/5")
    rk[1].metric("Đơn được đánh giá", f"{rated_count:,}")
    rk[2].metric("Tỷ lệ đánh giá thấp (1–2 sao)", f"{negative_count / rated_count * 100:.1f}%" if rated_count else "0.0%")
    r1, r2 = st.columns(2)
    plotly_chart(r1, px.pie(rating_data, names="Điểm đánh giá", values="Số đơn hàng", hole=0.45, title="Tỷ trọng đánh giá theo số sao"), use_container_width=True)
    plotly_chart(r2, px.bar(rating_data, x="Điểm đánh giá", y="Số đơn hàng", text="Số đơn hàng", title="Số lượng đơn theo điểm đánh giá"), use_container_width=True)

    st.markdown("#### Các đánh giá thấp cần chú ý")
    bad_reviews = selected_reviews[selected_reviews["review_score"] <= 2].copy()
    if not bad_reviews.empty:
        # Bổ sung thông tin đơn hàng từ orders và giá trị/vùng địa lý từ fact items đã nối dimension.
        context_cols = [c for c in ["order_id", "order_purchase_timestamp", "order_status", "delivery_days", "delivery_delay_days"] if c in order_view.columns]
        bad_reviews = bad_reviews.merge(order_view[context_cols].drop_duplicates("order_id"), on="order_id", how="left")
        item_context = d[d["seller_id"].astype(str) == selected_shop].copy()
        item_context = item_context[item_context["order_id"].astype(str).isin(bad_reviews["order_id"].astype(str))]
        item_context = item_context.groupby("order_id", as_index=False).agg(
            order_value=("revenue", "sum"), customer_city=("customer_city", "first"),
            customer_state=("customer_state", "first")
        )
        bad_reviews = bad_reviews.merge(item_context, on="order_id", how="left")
        title_col = next((c for c in ["review_comment_title", "comment_title", "review_title"] if c in bad_reviews.columns), None)
        message_col = next((c for c in ["review_comment_message", "comment_message", "review_comment", "comment"] if c in bad_reviews.columns), None)
        bad_display = pd.DataFrame({
            "Mã đơn hàng": bad_reviews["order_id"].astype(str),
            "Điểm đánh giá": bad_reviews["review_score"],
            "Giá trị đơn hàng (BRL)": bad_reviews["order_value"].fillna(0),
            "Thành phố": bad_reviews["customer_city"].fillna("Không rõ"),
            "Bang / Khu vực": bad_reviews["customer_state"].fillna("Không rõ")
        })
        if "order_purchase_timestamp" in bad_reviews.columns:
            bad_display["Ngày đặt hàng"] = pd.to_datetime(bad_reviews["order_purchase_timestamp"], errors="coerce").dt.strftime("%d/%m/%Y")
        if title_col:
            bad_display["Tiêu đề đánh giá"] = bad_reviews[title_col].fillna("").astype(str)
        if message_col:
            bad_display["Nội dung / nguyên nhân khách phản ánh"] = bad_reviews[message_col].fillna("").astype(str)
        if "delivery_delay_days" in bad_reviews.columns:
            bad_display["Số ngày giao trễ"] = bad_reviews["delivery_delay_days"]
        if "order_status" in bad_reviews.columns:
            bad_display["Trạng thái đơn"] = bad_reviews["order_status"]
        st.dataframe(bad_display, use_container_width=True, hide_index=True,
                     column_config={"Giá trị đơn hàng (BRL)": st.column_config.NumberColumn(format="R$ %.2f")})
    else:
        st.info("Không có đánh giá 1–2 sao trong khoảng thời gian này.")

    # Tra cứu chi tiết theo mã đơn hàng: chỉ đơn của shop và kỳ đang lọc.
    st.divider()
    st.markdown("### Tra cứu chi tiết đơn hàng")
    lookup_order_id = st.text_input("Nhập mã đơn hàng", key="ops_order_lookup", placeholder="Nhập order_id...").strip()
    if lookup_order_id:
        matched_order = order_view[order_view["order_id"].astype(str) == lookup_order_id]
        matched_items = d[(d["seller_id"].astype(str) == selected_shop) & (d["order_id"].astype(str) == lookup_order_id)].copy()
        if matched_order.empty or matched_items.empty:
            st.warning("Không tìm thấy mã đơn hàng trong shop và khoảng thời gian đang chọn.")
        else:
            order_row = matched_order.iloc[0]
            customer_row = matched_items.iloc[0]
            st.markdown("#### Thông tin khách hàng và vị trí địa lý")
            customer_info = pd.DataFrame([{
                "Mã đơn hàng": lookup_order_id,
                "Mã khách hàng": customer_row.get("customer_unique_id", "Không rõ"),
                "Thành phố": customer_row.get("customer_city", "Không rõ"),
                "Bang / Khu vực": customer_row.get("customer_state", "Không rõ"),
                "Vĩ độ": customer_row.get("latitude", np.nan),
                "Kinh độ": customer_row.get("longitude", np.nan),
                "Trạng thái đơn": order_row.get("order_status", "Không rõ"),
                "Ngày đặt hàng": order_row.get("order_purchase_timestamp", pd.NaT)
            }])
            st.dataframe(customer_info, use_container_width=True, hide_index=True)
            st.markdown("#### Danh sách sản phẩm trong đơn hàng")
            product_info = matched_items.groupby(["product_id", "product_category_name_english"], dropna=False, as_index=False).agg(
                quantity=("item_key", "count"), product_value=("price", "sum")
            ).rename(columns={"product_id": "Mã sản phẩm", "product_category_name_english": "Danh mục sản phẩm", "quantity": "Số lượng", "product_value": "Giá trị sản phẩm (BRL)"})
            st.dataframe(product_info, use_container_width=True, hide_index=True,
                         column_config={"Giá trị sản phẩm (BRL)": st.column_config.NumberColumn(format="R$ %.2f")})
            st.metric("Tổng giá trị sản phẩm trong đơn", money(matched_items["revenue"].sum()))

    # Phân tích phân bố điểm đánh giá theo khu vực và danh mục sản phẩm.
    st.divider()
    rating_reds = ["#560B14", "#86151C", "#B62930", "#DB4C4D", "#F28B82"]
    rating_red_by_score = dict(zip(("1", "2", "3", "4", "5"), rating_reds))
    st.markdown("### Số lượng đánh giá theo khu vực đặt hàng")
    if not selected_reviews.empty:
        review_geo = selected_reviews[["order_id", "review_score"]].merge(
            d[d["seller_id"].astype(str) == selected_shop][["order_id", "customer_state"]].drop_duplicates("order_id"),
            on="order_id", how="left"
        ).dropna(subset=["customer_state"])
        if not review_geo.empty:
            geo_rating = review_geo.groupby(["customer_state", "review_score"])["order_id"].nunique().reset_index(name="Số đơn hàng")
            geo_rating = geo_rating.rename(columns={"customer_state": "Bang / Khu vực", "review_score": "Điểm đánh giá"})
            plotly_chart(st, px.bar(geo_rating, x="Bang / Khu vực", y="Số đơn hàng", color="Điểm đánh giá", color_continuous_scale=rating_reds, color_discrete_map=rating_red_by_score, barmode="group", title="Số lượng từng mức đánh giá theo khu vực đặt hàng"), use_container_width=True)
        else:
            st.info("Không có thông tin khu vực để phân tích đánh giá; số lượng đánh giá theo khu vực là 0.")
    else:
        st.info("Không có đánh giá trong kỳ; số lượng đánh giá theo từng khu vực là 0.")

    st.markdown("### Số lượng đánh giá theo danh mục sản phẩm")
    if not selected_reviews.empty:
        review_category = selected_reviews[["order_id", "review_score"]].merge(
            d[d["seller_id"].astype(str) == selected_shop][["order_id", "product_category_name_english"]].drop_duplicates(),
            on="order_id", how="inner"
        ).dropna(subset=["product_category_name_english"])
        if not review_category.empty:
            category_rating = review_category.groupby(["product_category_name_english", "review_score"])["order_id"].nunique().reset_index(name="Số đơn hàng")
            category_rating = category_rating.rename(columns={"product_category_name_english": "Danh mục sản phẩm", "review_score": "Điểm đánh giá"})
            plotly_chart(st, px.bar(category_rating, x="Danh mục sản phẩm", y="Số đơn hàng", color="Điểm đánh giá", color_continuous_scale=rating_reds, color_discrete_map=rating_red_by_score, barmode="group", title="Số lượng từng mức đánh giá theo danh mục sản phẩm"), use_container_width=True)
        else:
            st.info("Không có dữ liệu danh mục để phân tích đánh giá; số lượng theo danh mục là 0.")
    else:
        st.info("Không có đánh giá trong kỳ; số lượng đánh giá theo danh mục sản phẩm là 0.")

st.divider()

