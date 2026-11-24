# Phân hệ EDA — giải trình cho dashboard Power BI

EDA là nơi khảo sát sâu và **giải trình** cho dashboard Power BI *Olist Market Analysis*: mỗi notebook ứng với đúng một trang,
đặt câu hỏi gắn với một trong 4 quyết định của CEO trước cuộc họp HĐQT, đưa bằng chứng từ Silver, đối chiếu số liệu với Gold
và chỉ rõ biểu đồ nào đứng sau visual nào trên trang, biểu đồ nào là phụ lục để trả lời chất vấn.

## 1. Điều kiện chạy

- Lakehouse đã chạy tới stage `gold` (`olist-lakehouse/run_lakehouse.bat`): notebook đọc `iceberg.silver.*` và đối chiếu với `iceberg.gold.*`.
- Trino ở `localhost:8085` (container `olist-trino`).
- Python có `trino`, `pandas`, `numpy`, `matplotlib`, `seaborn` (môi trường conda `vizdata` đang dùng). Kernel chạy với thư mục làm việc là `eda/`
  để `from common import *` hoạt động.
- Cấu hình qua biến môi trường (đều có mặc định): `TRINO_HOST`, `TRINO_PORT`, `TRINO_USER`, `TRINO_CATALOG`, `SILVER_SCHEMA`, `EDA_SCHEMA`,
  `OLIST_GOLD_CONFIG` (đường dẫn `olist-lakehouse/layers/gold/market/config.py` nếu lakehouse không nằm cạnh `serving/`).

**Chạy tuần tự từng notebook.** Bảng trung gian là bảng Iceberg trong schema `iceberg.eda` (REST catalog không có view) và các notebook dùng chung
tên bảng (`order_base`, `item_ps`…). Muốn chạy song song thì đặt `EDA_SCHEMA` khác nhau cho từng notebook.

## 2. Notebook ↔ trang Power BI

| Notebook | Trang Power BI | Quyết định |
|---|---|---|
| `01_executive_overview.ipynb` | p1 "Tổng quan kinh doanh" | Bức tranh chung trước 4 quyết định |
| `02_geo.ipynb` | p7 "Khu vực" | ① Đẩy ngân sách vào khu vực nào |
| `03_product.ipynb` | p2 "Ngành hàng" | ② Tập trung ngành hàng nào |
| `04_customer.ipynb` | "Khách hàng" (`18a7674d085b133e861b`) | ③ Nhắm nhóm khách nào, có duyệt chương trình kích cầu không |
| `05_seller.ipynb` | p6 "Người bán" | ④ Mở rộng thu hút nhóm người bán nào |

Mỗi notebook có cùng khung:
1. Bảng câu hỏi ở đầu; mỗi mục đi theo *câu hỏi → bằng chứng (truy vấn + biểu đồ) → **🔎 Kết quả chính***.
   Mọi con số trong phần kết quả lấy từ output của lần chạy, không điền trước.
2. Mục **Đối chiếu với Gold**: tính lại các thẻ KPI của trang từ Silver và `assert` khớp `iceberg.gold.*` (`common.reconcile`, sai lệch ≤ 0,5%).
3. Mục **Đánh giá & tuyển chọn visual**: bảng *biểu đồ EDA | câu hỏi / quyết định | visual Power BI tương ứng hoặc "Phụ lục" | lý do*.

## 3. Kỳ báo cáo (`common/period.py`)

Cùng quy tắc với Gold `report_period` và các measure DAX `Period Start/End`, `GMV QoQ %`:

| Khái niệm | Quy tắc |
|---|---|
| Tháng báo cáo | tháng mới nhất có số đơn hợp lệ ≥ `REPORT_MONTH_MIN_SHARE` (10%) × tháng cao nhất |
| Kỳ này | 01 → tháng báo cáo của năm mới nhất |
| Cùng kỳ (YoY) | cùng các tháng đó của năm trước |
| QoQ | quý chứa tháng báo cáo, từ max(đầu quý, đầu kỳ) → tháng báo cáo, so với cùng khoảng lùi 3 tháng |
| Cửa sổ xu hướng | 01/năm trước → tháng báo cáo (biểu đồ theo tháng) |

`connect()` trả về `P` (`Period`) và in ra kỳ đang dùng. Điều kiện SQL: `P.cur`, `P.prev`, `P.qtd`, `P.pqtd`, `P.in_period`
(trên cột `purchase_month`; `P.cur_on("i.purchase_month")` khi cần alias; `gold_period_where(...)` để dùng trên bảng Gold).

**Ghi đè kỳ** — tương đương CEO chọn khoảng ngày trên slicer Power BI, ví dụ review Q1/2018:

```powershell
$env:REPORT_START = "2018-01"; $env:REPORT_END = "2018-03"
```

Kỳ ghi đè phải nằm gọn trong một năm. Không đặt biến thì kỳ suy từ dữ liệu. **Không viết cứng năm / tháng / số liệu** trong notebook
(AGENTS.md §7.2.12). Trino 440 lỗi `DictionaryBlock` với `agg(x) FILTER (WHERE <điều kiện kỳ>)` → dùng `agg(CASE WHEN <điều kiện> THEN x END)`.

## 4. Quy tắc dùng chung với Gold và ngưỡng chỉ có ở EDA

Ngưỡng, band, phân khúc **đọc thẳng từ Gold** qua `common.GOLD` (`olist-lakehouse/layers/gold/market/config.py`), nên EDA và Power BI cùng một quy tắc:
đơn hợp lệ (`INVALID_ORDER_STATUSES`), tháng báo cáo, dân số bang, `STATE_MATRIX_MIN_PY_ORDERS`, `MIN_CATEGORY_SHARE`, `TOP_FLOW_CATEGORY_N`,
`SELLER_TIERS`, `SELLER_REVIEW_BANDS`, `SELLER_MIN_ORDERS`, `PRICE_BANDS`, `ORDER_VALUE_BANDS`, `DELAY_BANDS`, `GAP_BANDS`, `REPURCHASE_WINDOW_DAYS`, RFM.
Đổi quy tắc nghiệp vụ thì sửa ở Gold; EDA tự theo.

Ngưỡng **chỉ có ở EDA, không có trên Power BI** (khai báo ngay trong notebook dùng nó):

| Notebook | Ngưỡng |
|---|---|
| `03_product` §6 | tầng trọng lượng sản phẩm `WEIGHT_BINS` (< 0,5 / 2 / 5 / 10 kg) |
| `04_customer` §11 | tầng phí ship / giá trị hàng `FREIGHT_BINS`, tầng trả góp `INST_BINS` |
| `05_seller` §4–§5 | rủi ro review `RISK_REVIEW` = 3,8, phục vụ tốt `GOOD_REVIEW` = 4,0, tập trung top 3 `TOP3_HIGH` / `TOP3_MEDIUM` |
| `05_seller` §6 | **vùng đỏ**: % trễ > 20% hoặc review < 3,5; **ngôi sao**: % trễ ≤ 8% và review ≥ 4,2 (người bán ≥ `SELLER_MIN_ORDERS` đơn trong kỳ) |
| `05_seller` §8 | tầng thời gian bàn giao `HANDLING_BINS` |

Nếu một ngưỡng EDA tỏ ra hữu ích cho CEO, đề xuất đưa lên Gold thành việc riêng (sửa `config.py`, DDL Gold, ClickHouse, Power BI).

## 5. Gói `common/`

| Module | Nội dung |
|---|---|
| `config.py` | `GOLD` (nạp `olist-lakehouse/layers/gold/market/config.py`), quy tắc nghiệp vụ suy từ đó, bảng màu, 5 vùng |
| `db.py` | `TrinoSession`, `q`, `scalar`, `create_table_from_df` (kết nối ở `db.con`), `reconcile`, `gold_period_where` |
| `tables.py` | `connect` (chép Silver → `EDA_SCHEMA`, tạo `state_ref`, `order_base`, suy ra `Period`), `create_item_ps`, `create_order_payment` |
| `period.py` | `Period` (YoY, QoQ, ghi đè kỳ), `Kpi`, `kpi_table` |
| `sql.py` | `quantile_sql`, `median_sql`, `band_sql`; `band_bins`, `band_edges`, `band_order` (band của Gold → dạng notebook) |
| `rfm.py` | `create_customer_rfm` (giống `dim_customer_profile` của Gold), `SEG_ORDER` |
| `plotting.py` | theme, định dạng trục, chú thích vùng, `grouped_bar`, `band_labels`, `ordered` |

Chỉ thêm hàm vào `common/` khi từ hai notebook trở lên cần; logic riêng của một quyết định nằm trong notebook của nó.

## 6. Kiểm tra

```powershell
cd olist-eda\eda; python -c "from common import *; print('Common OK')"
```

Chạy hết 5 notebook theo thứ tự (ví dụ `jupyter nbconvert --to notebook --execute --inplace 01_executive_overview.ipynb`, …):
mọi ô chạy sạch và mọi ô **Đối chiếu với Gold** không báo `AssertionError`. Notebook được commit **kèm output** vì biểu đồ chính là bằng chứng giải trình.
