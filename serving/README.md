# Serving Layer (`serving/`)

Tầng tiêu thụ dữ liệu thống nhất cho nền tảng Olist Lakehouse, gồm 2 nhánh ứng dụng độc lập đọc từ lớp phục vụ ClickHouse và Trino của `olist-lakehouse/`:

| Nhánh | Thư mục | Công cụ | Nguồn dữ liệu |
|---|---|---|---|
| **Market Analytics & Executive BI** | `serving/olist-eda/` | Power BI PBIP (5 trang, 17 bảng model) + 5 Jupyter EDA Notebooks | ClickHouse `olist_gold` (`:8123`, HTTP `GoldTable`) + Trino `iceberg.silver` (3NF) & `iceberg.gold` (`:8085`) |
| **Seller Intelligence Dashboard** | `serving/seller-dashboard/` | Streamlit + Plotly (5 phân hệ quản trị cửa hàng) | ClickHouse `olist_gold_seller` (`:8123`, `clickhouse-connect`, 27 bảng Gold Seller) |

## Cách chạy nhanh

### 1. Market Analytics (`serving/olist-eda/`)
- **Power BI:** Mở `serving/olist-eda/powerbi/Olist Market Analysis.pbip` bằng Power BI Desktop (cần ClickHouse `:8123` đang chạy và đã nạp `olist_gold`).
- **EDA Notebooks:** Xem hướng dẫn chi tiết tại `serving/olist-eda/eda/README.md`.

### 2. Seller Dashboard (`serving/seller-dashboard/`)
```bash
cd serving/seller-dashboard
cp .env.example .env            # điền CLICKHOUSE_PASSWORD trùng với olist-lakehouse/infrastructure/.env
pip install -r requirements.txt
streamlit run Seller_dashboard.py
```
