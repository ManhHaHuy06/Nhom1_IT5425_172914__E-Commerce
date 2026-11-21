# Kế hoạch tái cấu trúc phân hệ EDA (notebook + `eda/common/`)

> **Mục tiêu:** EDA là nơi khảo sát và **giải trình** cho dashboard Power BI đã dựng. Mỗi notebook ứng với đúng một
> trang Power BI, đặt câu hỏi gắn với một trong 4 quyết định của CEO, đưa bằng chứng từ Silver, và cuối notebook
> chỉ rõ biểu đồ nào đứng sau visual nào trên trang, biểu đồ nào là phụ lục để trả lời chất vấn.
> Đợt này **không sửa Power BI, Gold hay ClickHouse**.

---

## 1. Bối cảnh

- CEO họp HĐQT mỗi quý để review kinh doanh và chốt 4 quyết định:
  ① đẩy ngân sách vào khu vực nào · ② tập trung ngành hàng nào · ③ nhắm nhóm khách hàng nào (hành vi mua, thanh toán,
  có duyệt chương trình kích cầu/chăm sóc không) · ④ mở rộng thu hút nhóm người bán nào.
- Trước khi có hệ thống: 2–3 nhân viên mất 4–6 ngày công gom số liệu bằng Excel; bị hỏi sâu thì không có số phân rã.
- Sau khi có hệ thống: CEO xem Power BI trong 15–30 phút; analyst dùng notebook EDA để đào sâu và trả lời chất vấn.
- Đây là **bối cảnh kể chuyện**, không phải tiêu chí nghiệm thu. Tiêu chí nghiệm thu ở §7.

## 2. Nguyên tắc

1. **Không viết cứng kỳ hay số liệu.** Kỳ suy từ dữ liệu theo đúng quy tắc của Gold `report_period` và DAX
   (AGENTS.md §7.2.12). Mọi con số trong markdown của notebook đều lấy từ kết quả chạy, không điền trước.
2. **Cùng quy tắc với Gold.** Ngưỡng, band, phân khúc đọc từ `GOLD` (`olist-lakehouse/layers/gold/config.py`) qua
   `common/config.py`. Ngưỡng chỉ có ở EDA phải khai báo trong notebook và ghi rõ "ngưỡng EDA, không có trên Power BI".
3. **Mỗi biểu đồ trả lời một câu hỏi** gắn với một quyết định, và phải có dữ liệu thật trong Silver. Không đặt chỉ
   tiêu số biểu đồ; thực tế khoảng 6–10 biểu đồ chính mỗi notebook, cộng phụ lục.
4. **Khung mỗi mục:** câu hỏi / giả thuyết → bằng chứng (truy vấn + biểu đồ) → kết luận ngắn rút từ số liệu.
5. **Refactor tại chỗ:** đổi tên bằng `git mv`, giữ truy vấn đang chạy được, sắp lại theo khung trên.

## 3. Kỳ báo cáo (`common/period.py`)

| Khái niệm | Quy tắc (giống DAX `order_summary.tmdl`) |
|---|---|
| Tháng báo cáo | tháng mới nhất có số đơn hợp lệ ≥ `REPORT_MONTH_MIN_SHARE` × tháng cao nhất |
| Kỳ này (mặc định) | 01 → tháng báo cáo của năm mới nhất |
| Cùng kỳ (YoY) | cùng các tháng đó của năm trước |
| QoQ (**mới**) | quý chứa ngày cuối kỳ, từ max(đầu quý, đầu kỳ) → cuối kỳ, so với cùng khoảng ngày lùi 3 tháng |
| Cửa sổ xu hướng | 01/năm trước → tháng báo cáo |
| Ghi đè (**mới**) | biến môi trường `REPORT_START=YYYY-MM`, `REPORT_END=YYYY-MM` thay kỳ này (tương đương CEO chọn khoảng ngày trên slicer, vd. Q1/2018); không đặt thì suy từ dữ liệu |

## 4. `eda/common/` — giữ cấu trúc phẳng

| File | Thay đổi |
|---|---|
| `config.py` | giữ nguyên (bộ nạp `GOLD`, màu, vùng) |
| `db.py` | giữ nguyên |
| `tables.py` | giữ; bổ sung bảng trung gian nếu từ 2 notebook trở lên cần |
| `period.py` | thêm QoQ + ghi đè kỳ; **nhận `kpi.py`** (`Kpi`, `kpi_table`, thêm cột QoQ) |
| `sql.py` | **nhận `bands.py`** (`band_bins`, `band_edges`, `band_order`) |
| `rfm.py`, `plotting.py` | giữ nguyên |
| `bands.py`, `kpi.py` | xoá sau khi gộp |
| `__init__.py` | cập nhật import; giao diện `from common import *` không đổi |

Không tạo `core/`, `decisions/`, `viz/`, `selection.py`. Logic riêng của một quyết định nằm trong notebook của nó.

## 5. Notebook ↔ trang Power BI

| Notebook mới | Nguồn | Trang Power BI |
|---|---|---|
| `01_executive_overview.ipynb` | `executive_overview.ipynb` | p1 "Tổng quan kinh doanh" |
| `02_geo.ipynb` | `geo.ipynb` + `customer_experience` §5 | p7 "Khu vực" |
| `03_product.ipynb` | `product.ipynb` + `customer_experience` §6 | p2 "Ngành hàng" |
| `04_customer.ipynb` | `customer_behaviour.ipynb` + `customer_experience` §2–4, §8–10 | 18a7674d085b133e861b "Khách hàng" |
| `05_seller.ipynb` | `seller.ipynb` + `customer_experience` §7 | p6 "Người bán" |

`customer_experience.ipynb` bị xoá sau khi mọi mục đã có chỗ mới.

### 5.1 `01_executive_overview` — sức khoẻ tổng thể
- **Giữ:** §1 KPI điều hành (thêm QoQ), §2 doanh thu theo tháng, §3 tệp khách RFM, §4 chất lượng phục vụ (% trễ, review TB theo tháng).
- **Thêm:** so sánh doanh thu cùng tháng kỳ này vs cùng kỳ; *(phụ lục)* waterfall tách tăng trưởng GMV thành số đơn × AOV.
- **Chuyển đi:** histogram giá trị đơn → `04` (giỏ hàng); heatmap tương quan → `04` (yếu tố gắn với không hài lòng).
- **Bỏ:** cohort doanh thu khách cũ/mới (trùng `04`).

### 5.2 `02_geo` — Quyết định ①
- **Giữ:** §1–10 hiện có (KPI, quy mô bang, vùng, giá trị khách, tăng trưởng, thành phố & bản đồ, cung–cầu, phí ship, cơ cấu ngành theo vùng, ma trận cơ hội).
- **Nhận:** `customer_experience` §5, gồm drill-down bang có % trễ / review tệ nhất (bang do dữ liệu chọn, không viết cứng).
- **Thêm:** boxplot số ngày giao theo vùng.
- **Bỏ:** Sankey/chord liên vùng (stacked bar nội/liên vùng ở §7 đã đủ); scatter phí ship × review 27 bang (tương quan sinh thái yếu).

### 5.3 `03_product` — Quyết định ②
- **Giữ:** §1–6 (KPI, cơ cấu ngành, ma trận tăng trưởng, cơ cấu theo tháng, phân khúc giá, đuôi dài).
- **Nhận:** `customer_experience` §6 (review, % trễ theo ngành).
- **Thêm:** % phí ship / giá theo `GOLD.PRICE_BANDS`; boxplot trọng lượng theo ngành với giả thuyết "hàng cồng kềnh gắn với giao trễ" (không có dữ liệu hư hỏng).
- **Bỏ:** đường mùa vụ "Ngày của Mẹ".

### 5.4 `04_customer` — Quyết định ③
- **Từ `customer_behaviour`:** §1–7 (KPI, khách mới/quay lại, RFM, thời điểm mua, giỏ hàng + histogram giá trị đơn, thanh toán & trả góp, mua lặp lại).
- **Từ `customer_experience`:** §2–4 (hài lòng, xu hướng trải nghiệm, giao hàng), §8 (đặc điểm đơn & thanh toán), §9 (đơn đầu → quay lại), §10 (yếu tố gắn với không hài lòng + heatmap tương quan).
- **Chương trình kích cầu:** trả lời bằng bằng chứng mô tả (cửa sổ `GOLD.REPURCHASE_WINDOW_DAYS`, tác động đơn đầu) và một con số quy mô tệp khách đủ điều kiện; không dựng mô hình doanh thu.
- **Bỏ:** "1 sao theo nguyên nhân" (không có trường nguyên nhân), mô phỏng tài chính chương trình.

### 5.5 `05_seller` — Quyết định ④
- **Giữ:** §1–5 (KPI & năng suất, phân tầng doanh thu, vòng đời/cohort, người bán × ngành, doanh thu × chất lượng).
- **Nhận:** `customer_experience` §7.
- **Thêm:** phân bố thời gian bàn giao (`order_delivered_carrier_date − order_approved_at`); scatter/hexbin số đơn × review.
- **Tầng người bán:** đúng `GOLD.SELLER_TIERS`.
- **Ngưỡng EDA:** nhóm "vùng đỏ" và "ngôi sao" khai báo trong notebook, chỉ áp cho người bán có ≥ `GOLD.SELLER_MIN_ORDERS` đơn. Nếu thuyết phục, đề xuất đưa lên Gold thành việc riêng.

### 5.6 Mục cuối mỗi notebook
1. **Đối chiếu với Gold:** một ô so các thẻ KPI của trang tương ứng (GMV, số đơn hợp lệ, % trễ, review TB… trong kỳ)
   tính từ Silver với `iceberg.gold.*` qua Trino, có ngưỡng sai số và `assert`.
2. **Đánh giá & tuyển chọn:** bảng markdown

   | Biểu đồ EDA | Câu hỏi / quyết định | Visual Power BI tương ứng (tiêu đề) hoặc "Phụ lục" | Lý do |
   |---|---|---|---|

## 6. Danh mục thay đổi

- `[MODIFY] eda/common/period.py` (QoQ, ghi đè kỳ, nhận `kpi`), `sql.py` (nhận `bands`), `__init__.py`
- `[DELETE] eda/common/bands.py`, `eda/common/kpi.py`
- `[RENAME+MODIFY]` `executive_overview` → `01_executive_overview`, `geo` → `02_geo`, `product` → `03_product`,
  `customer_behaviour` → `04_customer`, `seller` → `05_seller` (`.ipynb`, dùng `git mv`)
- `[DELETE] eda/customer_experience.ipynb` (sau khi chia hết mục)
- `[NEW] eda/README.md`: điều kiện chạy (lakehouse tới stage `gold`, Trino `:8085`); bảng notebook ↔ trang; quy tắc
  kỳ + ghi đè + QoQ; quy tắc dùng chung `GOLD` và danh sách ngưỡng chỉ có ở EDA; chạy tuần tự vì chung `EDA_SCHEMA`.
- Ngoài phạm vi: `EXECUTIVE_PLAYBOOK.md` (làm sau khi notebook ra số thật), mọi thay đổi Power BI / Gold.

## 7. Thực hiện & nghiệm thu

Nhánh `refactor/eda`, mỗi bước một commit:
1. `common/` — kiểm tra `python -c "from common import *"` và `Period` khớp `iceberg.gold.report_period`.
2. `01` → `05`, mỗi notebook: chạy headless hết, ô đối chiếu Gold pass, commit **kèm output**.
3. Xoá `customer_experience.ipynb` khi mọi mục đã có chỗ mới.
4. `README.md`.

Nghiệm thu: 5 notebook chạy sạch theo thứ tự; mọi ô đối chiếu pass; mỗi notebook có bảng tuyển chọn trỏ tới visual
có thật trên trang tương ứng; không còn năm/tháng hay số liệu viết cứng trong code và markdown.
