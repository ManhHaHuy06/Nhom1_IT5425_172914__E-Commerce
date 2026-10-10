# Olist Data Source API v2

Dockerized FastAPI source simulator for the 9-file Olist Brazilian E-Commerce dataset.

## Architecture

### Group A — Transactional/event data

Daily/date-driven APIs:

- Orders: `GET /api/v1/orders/date/{date}`
- Order count: `GET /api/v1/orders/date/{date}/count`
- Order items: `GET /api/v1/order-items/order-date/{date}`
- Payments: `GET /api/v1/payments/order-date/{date}`
- Reviews: `GET /api/v1/reviews/answer-date/{date}`

The review ingestion date is based on `review_answer_timestamp`, because the crawler is intended to represent reviews answered by customers on that day.

### Group B — Master/dimension data

Master data is synchronized by version/change, not by date:

- Products
- Sellers
- Customers
- Geolocation
- Categories

First call:

`GET /api/v1/master/manifest`

Then compare the local crawler state with each dataset's `version`. If changed, call:

`GET /api/v1/{dataset}/changes?since_version=N`

### Simulation

Because the original Olist dataset is static, the server keeps the original CSV files unchanged and stores simulated changes as JSONL files under `simulation/`.

Simulation endpoints support INSERT/UPDATE/DELETE for master datasets. This lets you demonstrate incremental ingestion/CDC without modifying the original dataset.

Simulation state is persisted through the Docker volume `./simulation:/app/simulation`.

## Run

```powershell
docker compose up --build
```

Swagger:

`http://localhost:8000/docs`

Health:

`http://localhost:8000/health`

## Group A examples

```text
GET /api/v1/orders/date/2018-01-01
GET /api/v1/orders/date/2018-01-01/count
GET /api/v1/order-items/order-date/2018-01-01
GET /api/v1/payments/order-date/2018-01-01
GET /api/v1/reviews/answer-date/2018-01-01
```

`GET /api/v1/order-items/date/{date}` is retained as a backward-compatible alias for the earlier crawler.

## Group B examples

```text
GET /api/v1/master/manifest
GET /api/v1/products/metadata
GET /api/v1/products/changes?since_version=1
GET /api/v1/products/{product_id}

GET /api/v1/sellers/metadata
GET /api/v1/customers/metadata
GET /api/v1/geolocation/metadata
GET /api/v1/categories/metadata
```

## Simulation example

Add a product:

```http
POST /api/v1/simulation/products/add
Content-Type: application/json

{
  "data": {
    "product_category_name": "electronics",
    "product_name_lenght": 25,
    "product_description_lenght": 200,
    "product_photos_qty": 3,
    "product_weight_g": 500,
    "product_length_cm": 20,
    "product_height_cm": 10,
    "product_width_cm": 15
  }
}
```

The server increments the product version and row count and writes an INSERT change to `simulation/products_changes.jsonl`.

Update:

```http
POST /api/v1/simulation/products/update
Content-Type: application/json

{
  "key": "PRODUCT_ID",
  "changes": {
    "product_weight_g": 650
  }
}
```

Delete:

```http
POST /api/v1/simulation/products/delete
Content-Type: application/json

{
  "key": "PRODUCT_ID"
}
```

Reset a simulated dataset:

```text
POST /api/v1/simulation/reset/products
```

## Important design rule

The crawler never calls `/api/v1/simulation/*`. Those endpoints are only for manually creating source changes for testing. The crawler only consumes read/manifest/change APIs.
