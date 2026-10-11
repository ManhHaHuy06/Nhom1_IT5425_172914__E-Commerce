# Kế hoạch chi tiết: Source giả lập → Airflow → Data Lake

> Phạm vi: phần **host server giả lập** (OLTP DB + Simulator) và **Airflow scripts** đẩy dữ liệu vào **MinIO (Bronze)**.
> Phần Spark/Silver/Warehouse/BI nằm ngoài tài liệu này (xem `architecture.md`).

---

## 0. Kết quả khảo sát dữ liệu ảnh hưởng đến thiết kế

Các số liệu dưới đây được tính trực tiếp từ `Data/*.csv`:

| Phát hiện | Hệ quả thiết kế |
|---|---|
| Có **1.359** đơn có `carrier_date < approved_at` và **23** đơn có `delivered_customer < carrier_date` | Trạng thái đơn **không** theo thứ tự thời gian tuyệt đối → simulator không dùng state machine cứng, mà áp "sự kiện gán mốc thời gian" và chỉ cho trạng thái **tiến lên**, không lùi |
| Dataset **không có timestamp** cho `invoiced`/`processing`/`canceled`/`unavailable` | Phải đặt giả định (mục 3.3), ghi rõ trong báo cáo |
| **775** đơn không có `order_items`, **1** đơn không có payment | Không đặt FK bắt buộc "đơn phải có item"; chấp nhận đơn rỗng |
| `review_id` **trùng 814 dòng**, **551** đơn có nhiều hơn 1 review | Bảng reviews dùng **khóa thay thế** (auto-increment), không dùng `review_id` làm PK |
| Đơn phân bố rất lệch: 2016 chỉ ~330 đơn, 2017–2018 đỉnh ~6.500 đơn/tháng | Tốc độ replay cần tính theo *ngày dữ liệu* chứ không theo *số đơn* |
| 1 `customer_unique_id` có tới **17** `customer_id` | Customers insert **theo từng đơn** (đúng bản chất `customer_id` theo đơn) |
| `geolocation` ~1 triệu dòng, `products`/`sellers` ít thay đổi | Tách bảng **tham chiếu** (snapshot hằng ngày) khỏi bảng **giao dịch** (incremental) |

---

## 1. Kiến trúc phần Source + Ingestion

> **Điều chỉnh so với `architecture.md`:** Simulator chạy như **service nền (daemon)**, không phải DAG Airflow chạy mỗi phút. Airflow chỉ lo **ingestion** và các bước sau. Đây cũng là cách thực tế: web server luôn chạy, hệ thống dữ liệu "kéo" từ DB.

```mermaid
flowchart LR
    subgraph HOST["Docker Compose (máy host)"]
        direction LR
        CSV[("Data/*.csv<br/>mount read-only")]
        PREP["build_events.py<br/>tiền xử lý"]
        EV[("events.parquet")]
        SIM["Simulator (daemon)<br/>đồng hồ ảo + replay"]
        OLTP[("OLTP MySQL :3307<br/>3NF + updated_at")]
        AF["Airflow<br/>scheduler + webserver<br/>(LocalExecutor + Postgres)"]
        MINIO[("MinIO :9000/:9001<br/>bucket olist-lake")]
    end
    CSV --> PREP --> EV --> SIM
    CSV -->|"seed.py (1 lần)"| OLTP
    SIM -->|"SQL INSERT/UPDATE<br/>(PyMySQL)"| OLTP
    AF -->|"SQL SELECT incremental<br/>(SQLAlchemy + PyMySQL)"| OLTP
    AF -->|"S3 API (boto3)<br/>Parquet"| MINIO
```

| Dịch vụ | Image | Cổng host | Vai trò |
|---|---|---|---|
| `oltp-mysql` | `mysql:8.0` | 3307 | DB giả lập của website |
| `simulator` | build từ `./source` (python 3.11) | – | Phát lại đơn hàng theo đồng hồ ảo |
| `minio` | build từ `infra/minio` (MinIO dựng từ source, vì image chính thức không còn tải được) | 9000 (API), 9001 (console) | Data Lake |
| `minio-init` | `minio/mc` | – | Tạo bucket `olist-lake` và các prefix, chạy một lần |
| `airflow-postgres` | `postgres:15` | – | Metadata DB của Airflow |
| `airflow-init` | build từ `./airflow` | – | `db migrate`, tạo user admin, tạo Connections/Variables |
| `airflow-webserver`, `airflow-scheduler` | build từ `./airflow` | 8080 | UI và lập lịch (LocalExecutor) |

Ước tính tài nguyên: ~4–6 GB RAM cho cả stack (chưa tính Spark).

---

## 2. Thiết kế OLTP DB (nguồn)

### 2.1 Quy ước chung
- MySQL `time_zone = '+00:00'`; cột `updated_at` dùng `TIMESTAMP(3)` (mili-giây).
- **Thời gian nghiệp vụ** (purchase, delivered…) giữ nguyên `DATETIME` như CSV (giờ ảo của dữ liệu).
- **Thời gian hệ thống** `created_at`/`updated_at` là giờ thật (wall-clock) do DB tự gán → đây là cột Airflow dùng làm watermark.
- Bật binlog dạng ROW ngay từ đầu để sau này nâng cấp lên CDC (Debezium) mà không phải dựng lại DB.

### 2.2 DDL cốt lõi (rút gọn)

```sql
CREATE TABLE customers (
  customer_id VARCHAR(32) PRIMARY KEY,
  customer_unique_id VARCHAR(32) NOT NULL,
  customer_zip_code_prefix CHAR(5),
  customer_city VARCHAR(64), customer_state CHAR(2),
  created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  KEY idx_upd (updated_at)
);

CREATE TABLE orders (
  order_id VARCHAR(32) PRIMARY KEY,
  customer_id VARCHAR(32) NOT NULL,
  order_status VARCHAR(16) NOT NULL,
  order_purchase_timestamp DATETIME NOT NULL,
  order_approved_at DATETIME NULL,
  order_delivered_carrier_date DATETIME NULL,
  order_delivered_customer_date DATETIME NULL,
  order_estimated_delivery_date DATETIME NULL,
  created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  KEY idx_upd (updated_at),
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE order_items (
  order_id VARCHAR(32), order_item_id INT,
  product_id VARCHAR(32), seller_id VARCHAR(32),
  shipping_limit_date DATETIME, price DECIMAL(10,2), freight_value DECIMAL(10,2),
  created_at ..., updated_at ...,            -- giống trên
  PRIMARY KEY (order_id, order_item_id), KEY idx_upd (updated_at)
);

CREATE TABLE order_payments (
  order_id VARCHAR(32), payment_sequential INT,
  payment_type VARCHAR(16), payment_installments INT, payment_value DECIMAL(10,2),
  created_at ..., updated_at ...,
  PRIMARY KEY (order_id, payment_sequential), KEY idx_upd (updated_at)
);

CREATE TABLE order_reviews (
  review_pk BIGINT AUTO_INCREMENT PRIMARY KEY,   -- review_id trong CSV bị trùng
  review_id VARCHAR(32), order_id VARCHAR(32),
  review_score TINYINT, review_comment_title VARCHAR(255), review_comment_message TEXT,
  review_creation_date DATETIME, review_answer_timestamp DATETIME,
  created_at ..., updated_at ..., KEY idx_upd (updated_at), KEY idx_order (order_id)
);

-- Bảng tham chiếu (seed 1 lần): products, sellers, geolocation, product_category_name_translation
-- Bảng điều khiển simulator:
CREATE TABLE sim_state (
  id TINYINT PRIMARY KEY DEFAULT 1,
  virtual_ts DATETIME NOT NULL,          -- con trỏ sự kiện đã phát đến đâu
  speed DOUBLE NOT NULL, status VARCHAR(8) NOT NULL DEFAULT 'running',  -- running|paused
  updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
);
```

### 2.3 Phân quyền DB
| User | Quyền | Dùng bởi |
|---|---|---|
| `sim_rw` | INSERT/UPDATE trên bảng giao dịch + `sim_state` | Simulator |
| `etl_ro` | SELECT toàn bộ | Airflow |
| (CDC sau này) `cdc_user` | REPLICATION SLAVE, REPLICATION CLIENT | Debezium |

---

## 3. Simulator: thiết kế chi tiết

### 3.1 Các thành phần (thư mục `source/`)

| File | Việc |
|---|---|
| `build_events.py` | Đọc CSV → dựng bảng **sự kiện** đã sắp theo thời gian, ghi `events.parquet` |
| `seed.py` | Tạo schema, nạp bảng tham chiếu (`products`, `sellers`, `geolocation`, `category_translation`) |
| `clock.py` | Đồng hồ ảo |
| `run.py` | CLI: `bootstrap`, `live`, `pause`, `resume` |
| `db.py` | Kết nối, `apply_events(batch)` trong 1 transaction |

### 3.2 Đồng hồ ảo

```
virtual_now = V0 + speed × (wall_now − wall_start)
```
- Mỗi **tick** (1 giây thật): lấy tất cả sự kiện có `ts ≤ virtual_now` mà chưa phát → áp vào DB trong **một transaction** → cập nhật `sim_state.virtual_ts`.
- Dừng/khởi động lại: đọc `sim_state.virtual_ts` để **tiếp tục đúng chỗ cũ**.
- Mọi ghi dùng `INSERT ... ON DUPLICATE KEY UPDATE` → phát lại sự kiện không sinh dữ liệu trùng (idempotent).

Gợi ý tốc độ:

| Chế độ | `speed` | Ý nghĩa | Dùng khi |
|---|---|---|---|
| bootstrap | không giới hạn | nạp toàn bộ sự kiện `ts < V_start` ngay lập tức | tạo lịch sử ban đầu (vd. tới 2017-06-01) |
| live-demo | 1440 | 1 phút thật = 1 ngày dữ liệu → toàn bộ ~16 tháng còn lại chạy ~ 8 giờ | demo, chạy qua đêm |
| live-fast | 14400 | 1 phút thật = 10 ngày | kiểm thử nhanh |

Tải ước tính ở live-demo: khoảng 150–250 đơn mỗi ngày dữ liệu → ~3–5 INSERT/giây — nhẹ với MySQL.

### 3.3 Mô hình sự kiện

`build_events.py` sinh, cho mỗi đơn, các sự kiện sau (`ts` là giờ nghiệp vụ trong CSV):

| Sự kiện | `ts` lấy từ | Hành động SQL | Trạng thái đặt |
|---|---|---|---|
| `PURCHASE` | `order_purchase_timestamp` | INSERT `customers`, `orders`, `order_items`, `order_payments` | `created` |
| `APPROVED` | `order_approved_at` | UPDATE `order_approved_at` | `approved` |
| `SHIPPED` | `order_delivered_carrier_date` | UPDATE `order_delivered_carrier_date` | `shipped` |
| `DELIVERED` | `order_delivered_customer_date` | UPDATE `order_delivered_customer_date` | `delivered` |
| `REVIEW` | `review_answer_timestamp` | INSERT `order_reviews` | – |
| `TERMINAL` *(giả định)* | xem dưới | UPDATE `order_status` | `canceled`/`unavailable`/`invoiced`/`processing` |

**Giả định cho trạng thái không có timestamp** (nêu rõ trong báo cáo):
- Với mọi đơn mà trạng thái suy ra từ các mốc thời gian không khớp trạng thái cuối trong CSV (1.849 đơn: `canceled`, `unavailable`, `invoiced`, `processing`…), thêm sự kiện `TERMINAL` với `ts` = mốc lớn nhất đã biết của đơn + 1 giây, ép trạng thái về đúng giá trị CSV.
- Mọi sự kiện có `ts` sớm hơn `purchase` (CSV có 1.5k dòng như vậy) bị kéo về bằng `purchase`.
- Đơn có `order_status = 'created'` hay `'approved'` cuối cùng đơn giản là dừng ở trạng thái đó, không có sự kiện nào sau.

**Quy tắc "trạng thái chỉ tiến":** do dữ liệu có mốc không đơn điệu, mọi UPDATE trạng thái đều dùng:

```sql
UPDATE orders
SET order_approved_at = %s,
    order_status = IF(FIELD(order_status,'created','approved','invoiced','processing','shipped','delivered')
                      < FIELD('approved','created','approved','invoiced','processing','shipped','delivered'),
                      'approved', order_status)
WHERE order_id = %s;
```
`canceled`/`unavailable` luôn được áp dụng (là trạng thái kết thúc).

> Lưu ý: đơn thật có `status = delivered` nhưng thiếu `order_delivered_customer_date` (8 đơn) sẽ không có sự kiện `DELIVERED`. Cần quyết định: bỏ qua, hoặc đặt `ts = estimated_delivery_date`. Mặc định: dùng `estimated_delivery_date` để trạng thái cuối khớp CSV.

### 3.4 Khung mã `run.py` (rút gọn)

```python
def live(speed, start):
    state = db.load_state(default_ts=start)               # tiếp tục nếu đã chạy
    events = pq.read_table("events.parquet").to_pandas()  # đã sort theo ts
    cursor = events.ts.searchsorted(state.virtual_ts, side="right")
    wall0, v0 = time.time(), state.virtual_ts
    while True:
        if db.is_paused(): time.sleep(1); continue
        v_now = v0 + timedelta(seconds=(time.time() - wall0) * speed)
        end = events.ts.searchsorted(v_now, side="right")
        if end > cursor:
            db.apply_events(events.iloc[cursor:end], virtual_ts=v_now)  # 1 transaction
            cursor = end
        time.sleep(1)
```

### 3.5 REST API (tùy chọn, giai đoạn sau)
Bọc `apply_events` trong FastAPI (`POST /orders`, `PATCH /orders/{id}/status`, `POST /reviews`) và cho simulator gọi API thay vì ghi DB trực tiếp. Chỉ làm khi còn thời gian: tăng tính chân thực ("server website") nhưng không ảnh hưởng luồng dữ liệu phía sau.

---

## 4. Airflow: ingestion vào Data Lake

### 4.1 Hạ tầng Airflow
- Phiên bản 2.10.x, `LocalExecutor`, metadata trên Postgres, UTC.
- Image tự build (`airflow/Dockerfile`) thêm: `pymysql`, `sqlalchemy`, `pandas`, `pyarrow`, `boto3`. Dùng **PyMySQL qua SQLAlchemy** thay vì MySQL provider để khỏi phải cài thư viện C của `mysqlclient` trong image.
- **Connections/Variables** (tạo ở `airflow-init` từ `.env`, không hard-code):
  - `OLTP_URL` = `mysql+pymysql://etl_ro:***@oltp-mysql:3306/shop`
  - `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` (bỏ comment trong `.env`)
  - Variable `wm__<table>`: watermark của từng bảng.

### 4.2 Hai DAG ingestion

| DAG | Lịch | Bảng | Cách nạp |
|---|---|---|---|
| `ingest_oltp_to_bronze` | mỗi 15 phút | `customers`, `orders`, `order_items`, `order_payments`, `order_reviews` | **Incremental** theo `updated_at` |
| `ingest_reference_snapshot` | hằng ngày | `products`, `sellers`, `geolocation`, `category_translation` | **Full snapshot** (bản chụp toàn bộ, ghi theo `snapshot_date`) |

Cấu hình chung: `catchup=False`, `max_active_runs=1` (tránh hai lần chạy chồng nhau đọc cùng watermark), `retries=3`, `retry_delay=1 phút`.

### 4.3 Luồng task của `ingest_oltp_to_bronze`

```mermaid
flowchart LR
    A["get_window<br/>wm_from = Variable<br/>wm_to = NOW(DB) − 10s"] --> B["extract_load<br/>(mapped theo bảng)<br/>SELECT → Parquet → MinIO"]
    B --> C["write_manifest<br/>_manifests/batch_id=.../manifest.json"]
    C --> D["commit_watermark<br/>Variable ← wm_to"]
    D --> E["outlet Dataset<br/>bronze_ready"]
```

Điểm thiết kế quan trọng:

1. **Cửa sổ `(wm_from, wm_to]`**, trong đó `wm_to = NOW() của DB − 10 giây`. Độ trễ 10 giây tránh bỏ sót các dòng đang nằm trong transaction chưa commit (simulator commit mỗi giây nên 10 giây là dư).
2. **Commit watermark ở task cuối cùng**, chỉ khi mọi bảng đã ghi xong. Nếu task giữa chừng lỗi → watermark giữ nguyên → lần chạy lại đọc đúng cửa sổ cũ.
3. **Idempotent & không ghi đè nhầm:** `batch_id` lấy từ **đầu cửa sổ (`wm_from`)**. Đầu cửa sổ chỉ tiến lên khi commit, nên chạy lại cùng cửa sổ thì ghi đè đúng file của nó, còn hai run khác nhau không bao giờ trùng key. *(Bản đầu dùng `data_interval_end`: run theo lịch và run chạy tay cùng khoảng thời gian trùng key, file sau ghi đè dữ liệu trước → mất dữ liệu; đã phát hiện qua test và sửa.)*
4. **Batch rỗng:** không ghi file, nhưng vẫn ghi manifest và tiến watermark.
5. **Metadata thêm vào mỗi dòng:** `_ingested_at`, `_source_table`, `_batch_id`, `_op = 'upsert'`. *(Cách này không phân biệt được INSERT với UPDATE và không phát hiện được DELETE — simulator không xóa dữ liệu nên chấp nhận; CDC sẽ giải quyết ở giai đoạn nâng cao.)*
6. **Lần chạy đầu tiên:** watermark mặc định `1970-01-01` → kéo toàn bộ dữ liệu đã bootstrap, chia thành nhiều file `part-0000N.parquet` (mỗi 100.000 dòng) để không tràn RAM.

### 4.4 Khung mã DAG (rút gọn)

```python
TABLES = ["customers", "orders", "order_items", "order_payments", "order_reviews"]
BRONZE_READY = Dataset("s3://olist-lake/bronze/")

@dag(schedule="*/15 * * * *", start_date=pendulum.datetime(2026, 1, 1), catchup=False,
     max_active_runs=1, default_args={"retries": 3, "retry_delay": timedelta(minutes=1)})
def ingest_oltp_to_bronze():

    @task
    def get_window():
        eng = create_engine(Variable.get("OLTP_URL"))
        with eng.connect() as c:
            wm_to = c.execute(text("SELECT NOW(3) - INTERVAL 10 SECOND")).scalar()
        return {t: {"from": Variable.get(f"wm__{t}", "1970-01-01 00:00:00"),
                    "to": str(wm_to)} for t in TABLES}

    @task
    def extract_load(table: str, window: dict, **ctx):
        batch_id = ctx["data_interval_end"].strftime("%Y%m%dT%H%M%S")
        w = window[table]
        sql = text(f"SELECT * FROM {table} WHERE updated_at > :a AND updated_at <= :b ORDER BY updated_at")
        rows, part = 0, 0
        for chunk in pd.read_sql(sql, engine(), params={"a": w["from"], "b": w["to"]}, chunksize=100_000):
            chunk = chunk.assign(_ingested_at=utcnow(), _source_table=table, _batch_id=batch_id, _op="upsert")
            key = f"bronze/{table}/ingest_date={batch_id[:8]}/batch_id={batch_id}/part-{part:05d}.parquet"
            s3_put_parquet(chunk, bucket="olist-lake", key=key)   # boto3 → MinIO, ghi đè
            rows += len(chunk); part += 1
        return {"table": table, "rows": rows, "parts": part}

    @task
    def write_manifest(results, **ctx): ...        # JSON: bảng, số dòng, cửa sổ, đường dẫn

    @task(outlets=[BRONZE_READY])
    def commit_watermark(window, _manifest):
        for t, w in window.items():
            Variable.set(f"wm__{t}", w["to"])

    window = get_window()
    results = extract_load.partial(window=window).expand(table=TABLES)
    commit_watermark(window, write_manifest(results))

ingest_oltp_to_bronze()
```

### 4.5 Bố cục Bronze trong MinIO

```
s3://olist-lake/
└── bronze/
    ├── orders/ingest_date=2026-10-10/batch_id=20261010T101500/part-00000.parquet
    ├── order_items/...
    ├── order_payments/...
    ├── order_reviews/...
    ├── customers/...
    ├── ref_products/snapshot_date=2026-10-10/part-00000.parquet
    ├── ref_geolocation/snapshot_date=.../...
    └── _manifests/batch_id=20261010T101500/manifest.json
```
Bronze **chỉ ghi thêm, không sửa/xóa**. Việc chọn bản mới nhất của mỗi `order_id` là việc của bước Bronze → Silver (dedupe theo `updated_at`).

### 4.6 Vòng chạy end-to-end

```mermaid
sequenceDiagram
    participant SIM as Simulator
    participant DB as OLTP MySQL
    participant AF as Airflow (mỗi 15 phút)
    participant S3 as MinIO
    loop mỗi giây
        SIM->>DB: BEGIN; INSERT/UPDATE các sự kiện đến hạn; COMMIT
    end
    AF->>DB: NOW() − 10s → wm_to
    AF->>DB: SELECT ... WHERE updated_at ∈ (wm_from, wm_to]
    DB-->>AF: các dòng mới/đổi
    AF->>S3: PUT bronze/<table>/.../part-N.parquet
    AF->>S3: PUT _manifests/.../manifest.json
    AF->>AF: Variable wm__<table> ← wm_to
    AF-->>AF: phát Dataset bronze_ready (kích hoạt DAG Silver sau này)
```

---

## 5. Cấu trúc thư mục

```
Data_Viz/
├── docker-compose.yml
├── .env                  # KHÔNG commit;  .env.example để chia sẻ cho nhóm
├── Data/                 # CSV gốc
├── source/
│   ├── Dockerfile · requirements.txt
│   ├── sql/ 01_schema.sql · 02_users.sql
│   ├── build_events.py · seed.py · clock.py · db.py · run.py
│   └── events.parquet    # sinh ra (gitignore)
├── airflow/
│   ├── Dockerfile · requirements.txt
│   ├── dags/ ingest_oltp_to_bronze.py · ingest_reference_snapshot.py
│   └── include/ s3_utils.py · oltp.py
└── infra/ minio-init.sh
```

---

## 6. Kế hoạch thực hiện theo bước

| Bước | Việc làm | Định nghĩa hoàn thành (DoD) | Ước lượng |
|---|---|---|---|
| **1. Hạ tầng** | `docker-compose` cho `oltp-mysql`, `minio`, `minio-init`; bỏ comment `MINIO_*` trong `.env`, tạo `.env.example` | Vào được MinIO Console `:9001`, thấy bucket `olist-lake`; `mysql -P 3307` kết nối được | 0,5 ngày |
| **2. Schema + Seed** | `01_schema.sql`, `02_users.sql`, `seed.py` nạp bảng tham chiếu | Số dòng khớp CSV: products 32.951 · sellers 3.095 · category_translation 71 · geolocation 1.000.163 | 1 ngày |
| **3. Build events** | `build_events.py` + kiểm thử | `events.parquet` có đủ sự kiện; mọi đơn có đúng 1 `PURCHASE`; sắp xếp theo `ts` | 1 ngày |
| **4. Simulator** | `clock.py`, `db.py`, `run.py` (bootstrap + live + resume) | Chạy live-fast: số dòng tăng đều; dừng rồi chạy lại **không trùng, không mất**; trạng thái không bao giờ lùi | 2 ngày |
| **5. Đối chiếu Source** | Chạy bootstrap + live đến hết dữ liệu | Cuối cùng: orders **99.441**, order_items **112.650**, order_payments **103.886**, `order_status` phân bố khớp CSV (delivered 96.478…) | 0,5 ngày |
| **6. Airflow stack** | `Dockerfile`, compose, `airflow-init` tạo Connection/Variable | UI `:8080` lên, DAG hiển thị không lỗi import | 1 ngày |
| **7. DAG ingest incremental** | `ingest_oltp_to_bronze` | Chạy tay: Bronze có Parquet mỗi bảng; manifest đúng; watermark tiến | 2 ngày |
| **8. DAG reference + hardening** | `ingest_reference_snapshot`, retry, alert lỗi (email/log), timeout | Hai DAG chạy theo lịch ổn định ≥ 2 giờ liên tục | 1 ngày |
| **9. Kiểm thử & tài liệu** | Các kiểm thử ở mục 7, viết README chạy lại từ đầu bằng 1 lệnh | Người khác clone repo + `docker compose up` là chạy | 1 ngày |

Tổng: ~10 ngày làm việc cho một người; chia đôi (người A: bước 1–5, người B: bước 6–8) thì làm song song sau bước 1.

---

## 7. Kiểm thử và đối chiếu

| Kiểm thử | Cách làm | Kỳ vọng |
|---|---|---|
| **Đối chiếu số dòng** | So `COUNT(*)` OLTP với CSV (mục bước 5) | Khớp (order_reviews: 99.224 dòng) |
| **Idempotent simulator** | Kill container giữa chừng, chạy lại | Không trùng khóa, `sim_state` tiếp tục đúng chỗ |
| **Trạng thái không lùi** | Với 1.359 đơn có `carrier < approved` | `order_status` cuối cùng khớp CSV |
| **Idempotent ingestion** | Chạy lại một run đã xong (Clear task) | Cùng object key, cùng số dòng, không có file mới |
| **Không mất dữ liệu** | Kill `extract_load` giữa chừng | Watermark không tiến; lần chạy sau lấy đủ |
| **Đối chiếu Bronze ↔ OLTP** | Với mỗi bảng: số `order_id` khác nhau trong Bronze = số dòng OLTP | Khớp 100% |
| **Chống bỏ sót do trễ commit** | Chạy simulator ở live-fast + chạy DAG chồng nhịp | Không có `order_id` nào có trong OLTP mà thiếu trong Bronze |

---

## 8. Rủi ro và cách giảm thiểu

| Rủi ro | Giảm thiểu |
|---|---|
| Bỏ sót dòng do watermark (transaction commit muộn) | `wm_to = NOW() − 10s`, simulator commit nhỏ và đều; test ở mục 7 |
| `updated_at` bị sửa tay/không có index → quét toàn bảng | Index `idx_upd` trên mọi bảng; không UPDATE thủ công ngoài simulator |
| Batch bootstrap quá lớn (~100k dòng) | Đọc/ghi theo chunk 100.000 dòng |
| Hai run Airflow chồng nhau | `max_active_runs=1` |
| Mất đồng bộ tốc độ: DAG 15 phút nhưng simulator chạy quá nhanh | Chọn `speed` sao cho mỗi batch ≲ vài chục nghìn dòng; hoặc giảm lịch DAG khi demo |
| Lộ khóa MinIO/MySQL | Giữ ở `.env` (đã nằm ngoài git), dùng `.env.example` cho nhóm; user DB tách quyền |
| Máy yếu không đủ RAM | Tắt `airflow-webserver` khi không xem UI; giảm `speed`; dùng LocalExecutor |

---

## 9. Quyết định đang mở (đề xuất mặc định)

1. **Thời điểm bootstrap**: nạp sẵn lịch sử đến đâu? → *Mặc định 2017-06-01* để còn đủ dữ liệu chạy live.
2. **Tốc độ demo**: → *1440× (1 phút thật = 1 ngày dữ liệu)*.
3. **Đơn `delivered` thiếu ngày giao** (8 đơn): → *dùng `estimated_delivery_date`*.
4. **Dùng FastAPI giữa Simulator và DB?** → *Không ở giai đoạn đầu*, thêm sau nếu còn thời gian.
5. **Watermark lưu ở đâu?** → *Airflow Variable* cho đơn giản; nếu muốn "đúng chuẩn doanh nghiệp", chuyển sang bảng `etl_control` trong warehouse.
6. **CDC (Debezium + Kafka)**: → *Làm sau khi đường incremental chạy ổn*; DB đã bật binlog ROW nên không phải dựng lại.
