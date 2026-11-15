# Implementation Log – Source giả lập → Airflow → Data Lake (Bronze)

> Cập nhật: 2026-10-10. Đối chiếu với `plan_source_ingestion.md` (kế hoạch) và `architecture.md` (thiết kế).
> Phạm vi đã làm: bước 1–9 của plan. Chưa làm: Silver/Gold, Spark, Warehouse, BI, CDC, FastAPI.

---

## 1. Tóm tắt

| Hạng mục | Trạng thái |
|---|---|
| OLTP DB giả lập (MySQL, schema 3NF, `updated_at`) | Xong |
| Seeder bảng tham chiếu | Xong |
| Simulator (đồng hồ ảo, replay đơn hàng, resume được) | Xong |
| Đối chiếu OLTP với CSV gốc | Xong, khớp 100% |
| Airflow stack (LocalExecutor + Postgres) | Xong |
| DAG ingest incremental → Bronze (15 phút) | Xong |
| DAG snapshot bảng tham chiếu → Bronze (hằng ngày) | Xong |
| Kiểm thử chịu tải (simulator live + nhiều run chồng nhau) | Xong, Bronze khớp OLTP |
| Hạ tầng Data Lake (MinIO) | Xong (dựng từ source, xem mục 6) |
| CDC (Debezium + Kafka) | Chưa làm (mới bật binlog ROW) |
| REST API (FastAPI) | Chưa làm (tùy chọn) |
| Cảnh báo khi DAG lỗi, chạy ổn định ≥ 2 giờ | Chưa làm |

---

## 2. Hạ tầng (Docker Compose)

| Container | Vai trò | Cổng host |
|---|---|---|
| `oltp-mysql` (mysql:8.0) | DB giả lập của website; UTC, binlog ROW | 3307 |
| `minio` + `minio-init` | Data Lake, tạo bucket `olist-lake` và các prefix `bronze/ silver/ gold/ _quarantine/` | 9000 (API), 9001 (console) |
| `airflow-postgres` | Metadata DB của Airflow | – |
| `airflow-init`, `airflow-webserver`, `airflow-scheduler` | Airflow 2.10.5, LocalExecutor | 8080 |
| `simulator` (profile `sim`) | Chạy theo lệnh (`seed`, `build_events`, `run.py ...`) | – |

Cấu hình bí mật nằm trong `.env` (đã thêm `.gitignore`; `.env.example` là bản che giá trị cho nhóm).

---

## 3. Source layer

### 3.1 Schema OLTP (`source/sql/01_schema.sql`)
- 5 bảng giao dịch: `customers`, `orders`, `order_items`, `order_payments`, `order_reviews`; mọi bảng có `created_at`, `updated_at TIMESTAMP(3)` (tự cập nhật, có index `idx_upd`).
- 4 bảng tham chiếu: `products`, `sellers`, `geolocation`, `product_category_name_translation`.
- `sim_state`: lưu con trỏ thời gian ảo, tốc độ, trạng thái (`running`/`paused`) của simulator.
- `order_reviews` dùng khóa thay thế `review_pk` vì `review_id` trong CSV bị trùng (814 dòng).
- User: `sim_rw` (SELECT/INSERT/UPDATE/DELETE) và `etl_ro` (chỉ SELECT), tạo bởi `source/sql/02_users.sh`.

### 3.2 Seeder (`source/seed.py`)
Nạp 4 bảng tham chiếu, idempotent (`INSERT IGNORE`). Số dòng: category_translation 71, products 32.951, sellers 3.095, geolocation 1.000.163 – khớp CSV.

### 3.3 Bảng sự kiện (`source/build_events.py`)
Sinh `events.parquet` gồm **493.937** sự kiện, sắp theo thời gian:

| Loại | Số lượng | Hành động |
|---|---:|---|
| PURCHASE | 99.441 | INSERT customer, order, items, payments |
| APPROVED | 99.281 | UPDATE `order_approved_at` |
| SHIPPED | 97.658 | UPDATE `order_delivered_carrier_date` |
| DELIVERED | 96.484 | UPDATE `order_delivered_customer_date` |
| REVIEW | 99.224 | INSERT review |
| TERMINAL | 1.849 | Ép trạng thái cuối về đúng CSV |

Các quyết định xử lý dữ liệu:
- Sự kiện có mốc sớm hơn thời điểm mua (có trong CSV) bị kéo về bằng thời điểm mua.
- Dataset không có mốc thời gian cho `invoiced`, `processing`, `canceled`, `unavailable`: dùng sự kiện `TERMINAL` tại (mốc lớn nhất đã biết + 1 giây).
- 8 đơn `delivered` thiếu ngày giao: dùng `order_estimated_delivery_date` làm thời điểm phát sự kiện (ngày giao thực vẫn để NULL như CSV).
- Trạng thái chỉ được tiến lên (rank-guard bằng `FIELD(...)` trong SQL), vì 1.359 đơn có ngày giao cho vận chuyển sớm hơn ngày duyệt.

### 3.4 Simulator (`source/run.py`, `replay.py`, `clock.py`)

| Lệnh | Việc |
|---|---|
| `bootstrap --until <ngày>` | Phát lại mọi sự kiện đến ngày đó, nhanh nhất có thể |
| `live --speed N` | Phát theo đồng hồ ảo: `virtual_now = v0 + N × thời gian thật` (mặc định 1440 = 1 phút thật ↔ 1 ngày dữ liệu) |
| `pause` / `resume` / `status` | Điều khiển và xem trạng thái |

- Mỗi lần ghi là một transaction gồm cả cập nhật `sim_state`, nên dừng/chạy lại tiếp tục đúng chỗ.
- Mọi ghi đều idempotent (`INSERT IGNORE`, UPDATE có rank-guard).
- Hiện trạng: đã bootstrap đến 2017-06-01 (đang ở khoảng giữa tháng 6/2017 sau các lần chạy thử live); simulator đang **dừng**.

### 3.5 Đối chiếu OLTP với CSV (`source/verify_source.py`)
Replay toàn bộ (bootstrap đến 2018-12-31, 434.131 sự kiện còn lại chạy trong ~68 giây) rồi so sánh với CSV, **tất cả PASS**:
- Số dòng: customers 99.441 · orders 99.441 · order_items 112.650 · order_payments 103.886 · order_reviews 99.224.
- `order_status` giống hệt (delivered 96.478, shipped 1.107, canceled 625, unavailable 609, invoiced 314, processing 301, created 5, approved 2).
- 5 cột thời gian của `orders` giống hệt từng dòng.
- Tổng `price` 13.591.643,70 · tổng `freight_value` 2.251.909,54 · tổng `payment_value` · phân bố `review_score` đều khớp.

Sau đó đã reset bảng giao dịch và bootstrap lại đến 2017-06-01 để còn dữ liệu cho chế độ live.

---

## 4. Ingestion layer (Airflow → Bronze)

### 4.1 Airflow
- Image tự build (`airflow/Dockerfile`) thêm `pymysql`, `pyarrow`, `boto3`, `sqlalchemy`, `pandas` (dùng constraints chính thức của Airflow).
- Thông tin kết nối đưa vào Airflow Variables qua biến môi trường (`OLTP_URL`, `MINIO_*`), không hard-code.
- Helper dùng chung: `airflow/include/oltp.py` (engine SQLAlchemy), `airflow/include/s3_utils.py` (ghi Parquet/JSON lên MinIO; timestamp ép về microsecond để Spark đọc được).

### 4.2 DAG `ingest_oltp_to_bronze` (mỗi 15 phút)
Luồng: `get_window` → `extract_load` (mapped theo 5 bảng) → `write_manifest` → `commit_watermark` (phát Dataset `bronze_ready`).
- Cửa sổ `(wm_from, wm_to]`, với `wm_to = NOW() của DB − 10 giây` (tránh bỏ sót transaction chưa commit).
- Watermark (Airflow Variable `wm__<table>`) chỉ được commit sau khi mọi bảng và manifest đã ghi xong; lỗi giữa chừng thì lần sau đọc lại đúng cửa sổ cũ.
- Đọc theo stream, chia file mỗi 100.000 dòng; thêm metadata `_ingested_at`, `_source_table`, `_batch_id`, `_op` (hiện luôn là `upsert`).
- Cấu hình: `catchup=False`, `max_active_runs=1`, `retries=3`.
- Bố cục: `bronze/<table>/ingest_date=YYYYMMDD/batch_id=<wm_from>/part-NNNNN.parquet` và `bronze/_manifests/batch_id=<wm_to>/manifest.json`.

### 4.3 DAG `ingest_reference_snapshot` (hằng ngày)
Chụp toàn bộ 4 bảng tham chiếu vào `bronze/ref_{products,sellers,geolocation,category_translation}/snapshot_date=YYYY-MM-DD/`. Kết quả chạy thử thành công (geolocation ~46,7 MB chia 12 file).

---

## 5. Kiểm thử đã thực hiện

| Kiểm thử | Kết quả |
|---|---|
| Số dòng, trạng thái, mốc thời gian, tổng tiền OLTP so với CSV | PASS (mục 3.5) |
| Simulator live: dừng rồi chạy lại | Tiếp tục đúng chỗ, không trùng, không mất |
| Bronze so với OLTP sau lần nạp đầu | Khớp: orders 11.695 · customers 11.695 · items 13.096 · payments 12.434 · reviews 9.878 |
| **Chịu tải:** simulator live (tốc độ 14.400×) + 3 run Airflow chạy tay chồng nhau + 1 run cuối sau khi dừng | Bronze có 4 file/bảng, 15.715 dòng orders (gồm các lần cập nhật); sau khi gộp bản mới nhất theo `order_id` còn 13.773 = OLTP; `order_status` và `updated_at` khớp 100%; items 15.400 · payments 14.666 · reviews 11.883 · customers 13.773 đều khớp OLTP |

---

## 6. Sự cố gặp phải và cách xử lý

| Vấn đề | Cách xử lý |
|---|---|
| **Image MinIO chính thức không còn tải được** (Docker Hub từ chối truy cập; quay.io trả 401; `dl.min.io` trả 410) | Build MinIO server và `mc` từ source trong `infra/minio/Dockerfile` (multi-stage Go). Có đủ S3 API; nên ghi chú trong báo cáo |
| **Mất dữ liệu do trùng `batch_id`:** bản đầu lấy `batch_id` từ `data_interval_end`. Run theo lịch và run chạy tay trong cùng khoảng thời gian trùng key, file sau ghi đè dữ liệu trước trong khi watermark đã tiến lên | Phát hiện khi đối chiếu Bronze với OLTP (Bronze rỗng dù OLTP có 11.695 đơn). Đổi sang `batch_id` theo **đầu cửa sổ `wm_from`** (chỉ tăng khi commit). Xóa Bronze và watermark của lần thử, chạy lại và kiểm chứng ở mục 5 |
| Dữ liệu có sự kiện sớm hơn thời điểm mua (làm `build_events.py` lỗi `KeyError`) | Kéo mọi sự kiện về bằng thời điểm mua |
| Driver PyMySQL thiếu `cryptography` (xác thực `caching_sha2_password`) | Thêm vào `source/requirements.txt` |
| `.env` đang được git theo dõi (chứa credential Aiven) | Thêm `.gitignore`, `git rm --cached .env` (chưa commit). Repo không có remote; nếu từng đẩy đi nơi khác thì nên đổi mật khẩu Aiven |
| Số dòng `order_reviews` ghi sai là 104.719 (do `wc -l` đếm cả dòng xuống hàng trong nhận xét) | Sửa thành **99.224** trong `Data/data_description.md` |

---

## 7. Khác biệt so với `architecture.md` / plan (cần cập nhật tài liệu)

1. Simulator chạy như **service nền chạy theo lệnh**, không phải "DAG 1: simulate_orders" như mục 4 `architecture.md`.
2. Bronze có thêm tầng `batch_id=` và manifest; dữ liệu tham chiếu nằm ở `bronze/ref_*/snapshot_date=`.
3. Seeder chỉ nạp bảng tham chiếu; lịch sử ban đầu do `bootstrap` nạp.
4. Vòng đời đơn thực tế: `created → approved → shipped → delivered`, các trạng thái còn lại được ép bằng sự kiện `TERMINAL` (do dataset thiếu mốc thời gian).
5. Watermark lưu trong Airflow Variable (quyết định mở số 5 chọn phương án đơn giản).

---

## 8. Việc còn lại

| Ưu tiên | Việc |
|---|---|
| Cao | Spark Bronze → Silver (dedupe theo `updated_at`, ép kiểu, MERGE), data quality |
| Cao | Silver → Warehouse (star schema trên MySQL), Power BI |
| Trung bình | Cảnh báo khi DAG lỗi; chạy ổn định ≥ 2 giờ liên tục; đưa simulator vào chạy tự động (hoặc service riêng) |
| Trung bình | Cập nhật `architecture.md` cho khớp mục 7 |
| Thấp (điểm cộng) | CDC Debezium + Kafka; FastAPI giữa simulator và DB; lineage, Grafana |

---

## 9. Cách chạy lại

Xem `README.md`. Tóm tắt:

```bash
cp .env.example .env && docker compose up -d --build
docker compose --profile sim run --rm simulator python seed.py
docker compose --profile sim run --rm simulator python build_events.py
docker compose --profile sim run --rm simulator python run.py bootstrap --until 2017-06-01
docker compose --profile sim run -d --name sim-live simulator python run.py live --speed 1440
```

Airflow UI: http://localhost:8080 · MinIO console: http://localhost:9001 · OLTP MySQL: `localhost:3307`.
