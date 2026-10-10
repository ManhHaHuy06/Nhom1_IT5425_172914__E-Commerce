# Olist Crawler - Full Group A + Group B

Crawler cho Olist Data Source API v2.

## 1. Chiến lược

### Group A - transactional

Crawl theo ngày:

- orders: `/api/v1/orders/date/{date}`
- order_items: `/api/v1/order-items/order-date/{date}`
- payments: `/api/v1/payments/order-date/{date}`
- reviews: `/api/v1/reviews/answer-date/{date}`

`order_items` và `payments` lấy theo `order_purchase_timestamp` của order. Reviews lấy theo `review_answer_timestamp`.

Crawler mặc định lấy danh sách ngày thực tế từ `/api/v1/orders/dates`, lọc theo `START_DATE=2016-09-04` đến `END_DATE=2018-10-17`. Reviews có stream riêng theo `review_answer_timestamp`, mặc định `REVIEW_START_DATE=2016-09-04` đến `REVIEW_END_DATE=2018-10-29`.

### Group B - master/dimension

Các dataset:

- products
- sellers
- customers
- geolocation
- categories

Lần chạy đầu: bootstrap full snapshot theo trang và kiểm tra `row_count` với `/master/manifest`.

Các lần chạy sau: gọi `/master/manifest`, so sánh `version` + `etag`, và chỉ gọi `/changes?since_version=N` khi có thay đổi.

## 2. Chạy Docker

Data Source Server chạy trên Docker host ở port 8000:

```powershell
docker compose up --build
```

Nếu Data Source Server đã chạy bằng Docker Desktop trên Windows, `host.docker.internal:8000` là địa chỉ mặc định của crawler container.

## 3. Chạy lại

Crawler idempotent theo ngày và theo master version. State nằm ở:

```text
raw/_metadata/crawler_state.json
```

Không xóa `raw/` nếu muốn crawler tiếp tục từ state cũ.

## 4. Raw layout

```text
raw/olist/
├── orders/date=YYYY-MM-DD/orders.csv
├── order_items/date=YYYY-MM-DD/order_items.csv
├── payments/date=YYYY-MM-DD/payments.csv
├── reviews/date=YYYY-MM-DD/reviews.csv
├── reviews/unanswered/reviews_unanswered.csv
├── products/version=N/pages/part-*.csv
├── sellers/version=N/pages/part-*.csv
├── customers/version=N/pages/part-*.csv
├── geolocation/version=N/pages/part-*.csv
├── categories/version=N/pages/part-*.csv
└── _metadata/
    ├── crawler_state.json
    └── manifests/date=YYYY-MM-DD.json
```

## 5. Chạy riêng từng nhóm

Transaction only:

```powershell
$env:RUN_MASTER="false"
docker compose up --build
```

Master only:

```powershell
$env:RUN_TRANSACTIONS="false"
docker compose up --build
```

Incremental master sau khi đã bootstrap:

```powershell
$env:BOOTSTRAP_MASTER="false"
docker compose up --build
```

## 6. Mô phỏng thay đổi

Dùng Swagger của Data Source API:

`http://localhost:8000/docs`

Ví dụ POST:

```text
/api/v1/simulation/products/add
/api/v1/simulation/products/update
/api/v1/simulation/products/delete
```

Sau đó chạy crawler. Nó sẽ thấy version mới qua `/api/v1/master/manifest` và lấy change log qua `/api/v1/products/changes?since_version=N`.
