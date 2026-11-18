# Migration & Runbook — Unified Olist Lakehouse

## 1. Phạm vi và nguyên tắc

Đây là bản hợp nhất hai ZIP được cung cấp. Các phép biến đổi nghiệp vụ hiện có trong Lakehouse được giữ nguyên:
- `layers/bronze/create_bronze_tables.sql`
- `layers/silver/dimensions.py`, `layers/silver/facts.py`
- `layers/gold/build_marts.py`
- DDL Silver/Gold và cấu trúc 27 Gold marts.

Các phần mới được bổ sung:
- MySQL OLTP 3NF và dữ liệu mô phỏng.
- Airflow extraction/orchestration.
- Spark Bronze adapter cho OLTP.
- Spark Job Runner sử dụng Spark Master/Worker hiện có.
- ClickHouse staged refresh.
- Dashboard container kết nối ClickHouse nội bộ.

**Không có MinIO, Iceberg REST, ClickHouse hoặc Spark Master/Worker thứ hai.** Airflow, MySQL OLTP và Spark Job Runner cùng tham gia `olist-lakehouse-network`.

## 2. Lưu ý về dữ liệu nguồn trong ZIP

Hai ZIP đầu vào không chứa các file CSV Olist gốc. `Data/` của Integration chỉ có tài liệu mô tả dữ liệu; không có CSV/Parquet nguồn.

Vì vậy, `source/seed.py` tạo bộ dữ liệu Olist-like tổng hợp có quan hệ khóa hợp lệ:
- 5.000 customer rows, 500 products, 100 sellers.
- 1.200 zip prefixes với nhiều geolocation points.
- 12.000 historical orders mặc định, cùng order items, payments và reviews.
- Dữ liệu có ngày nghiệp vụ trong giai đoạn 2016–2018; simulator bắt đầu virtual time từ 2019.

Có thể tăng/giảm số lượng qua `SIM_INITIAL_ORDERS` trong `.env`. Nếu MySQL volume đã có orders, seed nhận diện và không ghi đè dữ liệu hiện có.

Đây là dữ liệu mô phỏng, không phải bản sao của Olist public dataset. Khi có bộ CSV gốc, cần bổ sung importer có kiểm tra schema và đối soát trước khi nạp.

## 3. Cấu trúc và luồng dữ liệu

### 3.1 Transactional — incremental

| MySQL | Bronze Iceberg |
|---|---|
| `customers` | `olist_customers` |
| `orders` | `olist_orders` |
| `order_items` | `olist_order_items` |
| `order_payments` | `olist_order_payments` |
| `order_reviews` | `olist_order_reviews` |

Cột `updated_at` trong OLTP tự cập nhật khi INSERT/UPDATE. Airflow dùng:

`W_old < updated_at <= W_new`

`W_new = NOW(3) - INTERVAL 10 SECOND`

Watermark được giữ riêng theo bảng trong Airflow Variables. Cửa sổ watermark chỉ được commit sau khi Bronze, Silver, Gold và ClickHouse hoàn thành khi có dữ liệu mới.

### 3.2 Reference — daily full snapshot

| MySQL | Bronze Iceberg |
|---|---|
| `products` | `olist_products` |
| `sellers` | `olist_sellers` |
| `geolocation` | `olist_geolocation` |
| `product_category_name_translation` | `product_category_name_translation` |

Mỗi ngày tạo `snapshot_id`, Parquet staging và manifest. Silver `latest()` hiện có tiếp tục chọn phiên bản mới nhất cho products/sellers/categories.

**Geolocation policy:** Silver hiện tại tổng hợp toàn bộ Bronze geolocation rows, không chọn latest snapshot. Do đó adapter:
- append snapshot nếu Bronze geolocation đang rỗng;
- bỏ qua nếu snapshot trùng chính xác;
- bảo toàn geolocation Bronze hiện tại và bỏ qua snapshot khác nội dung, thay vì append gây nhân bản/pha trộn dữ liệu.
- fingerprint snapshot vẫn được lưu; nếu nguồn geolocation thay đổi ở lần sau, extraction dừng an toàn. Muốn thay đổi geolocation cần một kế hoạch rebuild có kiểm soát, không tự động append.

### 3.3 Staging → Bronze

MinIO bucket `lakehouse`:
- `staging/bronze/oltp/batch_id=.../`
- `staging/bronze/reference/snapshot_id=.../`

Mỗi prefix chứa Parquet theo bảng và `manifest.json`. Staging tách biệt với Iceberg managed warehouse.

Spark adapter `layers/bronze/load_oltp_bronze.py`:
- xác thực cột bắt buộc;
- chỉ project business columns được DDL Bronze định nghĩa;
- cast business fields sang STRING;
- thêm `loaded_at`, `source_file`, `batch_id`;
- append-only, không tạo/alter bảng;
- kiểm tra batch đã nạp theo từng bảng trước append.

Đây là idempotency theo từng bảng, không phải transaction nguyên tử bao trùm 9 bảng.

### 3.4 Bronze → Silver → Gold → ClickHouse

Sau Bronze:
1. `layers/silver/dimensions.py` — chạy trước.
2. `layers/silver/facts.py` — chạy sau Dimensions.
3. `layers/gold/build_marts.py` — tạo và export 27 marts.
4. ClickHouse loader nạp Parquet từ `lakehouse/exports/gold/`.

Không viết lại transformation trong Airflow. DAG gọi `olist_pipeline.py` hiện có qua Spark Job Runner.

## 4. ClickHouse serving refresh

Loader mới không thực thi các lệnh `TRUNCATE TABLE` trong file SQL cũ. File cũ được lưu riêng tại `layers/clickhouse/load_gold_legacy_truncate.sql` để tham khảo, đã thay credential mặc định bằng placeholder và không được thực thi. `layers/clickhouse/load_gold.sql` hiện chỉ là ghi chú chuyển sang loader Airflow. Quy trình:
1. Kiểm tra `olist_gold` có engine `Atomic`.
2. Kiểm tra đúng đủ 27 bảng serving.
3. Tạo staging table cho từng bảng.
4. Nạp Parquet vào toàn bộ staging tables.
5. Chỉ sau khi tất cả staging load thành công mới `EXCHANGE TABLES` từng bảng.
6. Sau khi hoàn thành toàn bộ exchange, drop các bảng generation cũ đang mang tên staging.

Nếu staging load thất bại trước exchange, bảng serving cũ được giữ nguyên. Nếu lỗi xảy ra giữa chuỗi exchange, loader giữ nguyên staging tables và báo `CLICKHOUSE RECOVERY REQUIRED`; không tự xóa vì cần xác định bảng nào đã đổi generation. Exchange là atomic theo từng bảng, không phải một transaction atomic bao trùm cả 27 bảng.

## 5. Chuẩn bị môi trường

### 5.1 Tạo `.env`

Từ PowerShell tại thư mục dự án:

```powershell
.\setup.ps1
notepad .env
```

Nếu bạn có file `infrastructure/.env` cũ, script sẽ sao chép nó làm nền. Script cũng tự tìm file Integration `.env` ở đường dẫn sibling mặc định để nhập riêng các biến `OLTP_*`. Nếu Integration `.env` ở vị trí khác, truyền đường dẫn:

```powershell
.\setup.ps1 -IntegrationEnvPath "C:\duong-dan\Data_Viz\.env"
```

Nếu không tìm thấy các file cũ, script tạo `.env` từ `.env.example`.

**Bắt buộc nếu tái sử dụng Lakehouse volumes:** `POSTGRES_USER`, `POSTGRES_PASSWORD`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `CLICKHOUSE_PASSWORD` phải khớp credential đã dùng để khởi tạo các volume Lakehouse hiện tại. Không đổi credential của volume hiện có chỉ để làm theo ví dụ.

Đặt các biến OLTP/Airflow mới trong `.env`. Đổi mọi giá trị `CHANGE_ME...`; dùng password/token mạnh và URL-safe cho connection string Airflow.

## 6. Chuyển từ hai Compose cũ

Không chạy Compose cũ và Compose mới đồng thời vì container name của Lakehouse có chủ đích được giữ nguyên.

1. Chuẩn bị credential và dừng Integration Compose cũ từ thư mục Integration cũ:

```powershell
docker compose down
```

Không thêm `-v`. Unified Compose tái sử dụng volume MySQL `olist-integration_oltp_data` để giữ dữ liệu OLTP hiện có; các volume metadata/log/work của Airflow được tạo riêng để tránh mang theo watermark cũ có thể không an toàn.

2. Dừng Lakehouse Compose cũ từ thư mục `olist-lakehouse/infrastructure` cũ:

```powershell
docker compose down
```

Không thêm `-v`, không chạy `docker volume prune`. Các volume Lakehouse có tên cố định (`olist-lakehouse-minio-data`, `olist-lakehouse-postgres-data`, `olist-lakehouse-clickhouse-data`) được unified Compose khai báo dùng lại.

3. Đảm bảo `.env` ở thư mục gốc unified có credential đúng với các volume Lakehouse hiện có.

4. Từ thư mục gốc unified, kiểm tra cấu hình:

```powershell
docker compose config --quiet
docker compose config --services
```

5. Chỉ sau khi kiểm tra xong và bạn đồng ý chuyển stack, khởi động:

```powershell
docker compose up -d --build
```

Lần đầu có thể mất thời gian tải Spark/Iceberg dependencies và build Airflow/Runner images.

## 7. Thứ tự khởi tạo

Compose thực hiện theo dependency:
1. Lakehouse PostgreSQL, MinIO, Iceberg REST, Spark Master/Worker, Trino, ClickHouse.
2. MinIO safe init chỉ đảm bảo bucket `handoff` và `lakehouse` tồn tại; không xóa bucket nào.
3. ClickHouse init chạy `CREATE ... IF NOT EXISTS`.
4. Spark Job Runner healthcheck.
5. `lakehouse-ddl-init` gọi Lakehouse `olist_pipeline.py --stage ddl`; DDL hiện có dùng `CREATE ... IF NOT EXISTS`.
6. MySQL OLTP khởi tạo schema và users; `oltp-seed` tạo dữ liệu nếu database trống.
7. Airflow init migrate metadata DB và tạo admin.
8. Airflow Scheduler/Webserver và simulator chạy.
9. DAG tự chạy mỗi 15 phút; reference snapshot tối đa một lần mỗi logical date.

Không chạy DDL/bronze/silver/gold thủ công đồng thời với DAG vì cả hai cùng ghi Iceberg tables.

## 8. Kiểm tra sau khi khởi động

```powershell
docker compose ps
docker compose logs --tail 100 oltp-seed
docker compose logs --tail 100 simulator
docker compose logs --tail 100 airflow-scheduler
docker compose exec airflow-scheduler airflow dags list
```

Mở:
- Airflow: `http://localhost:8080`
- Dashboard: `http://localhost:8501`

DAG: `olist_end_to_end_pipeline`.

Simulator controls:

```powershell
docker compose exec simulator python simulator.py status
docker compose exec simulator python simulator.py pause
docker compose exec simulator python simulator.py resume
```

Có thể trigger DAG thủ công từ Airflow UI sau khi dịch vụ healthy. Không trigger đồng thời nhiều run; DAG `max_active_runs=1`.

## 9. Kiểm tra code không cần Docker

```powershell
python -m unittest discover -s airflow/tests -p "test_*.py" -v
python -m compileall -q airflow source layers spark_runner
docker compose config --quiet
```

Các kiểm tra này không kết nối/ghi dữ liệu.

## 10. Rollback

Nếu chỉ muốn dừng unified stack:

```powershell
docker compose down
```

Không dùng `-v`. Các volume vẫn được giữ nguyên.

Để quay lại Compose Lakehouse cũ, dùng Compose cũ với `.env` và named volumes cũ. Không xóa volume. Các bảng Bronze có thể đã nhận thêm batch mới nếu DAG đã chạy; không rollback bằng cách drop/truncate. Muốn quay lại dữ liệu cũ cần restore từ backup/snapshot Iceberg/ClickHouse theo quy trình riêng.

## 11. Những thao tác không tự động thực hiện

- Không xóa bucket MinIO hoặc volume.
- Không drop/truncate Iceberg tables.
- Không thay đổi DDL/transformations Silver/Gold.
- Không ghi dữ liệu vào ClickHouse live tables trước khi toàn bộ staging tables được nạp.
- Không tự động thay geolocation Bronze khi snapshot khác nội dung.
- Không chạy pipeline song song với manual `run_lakehouse.bat`.
