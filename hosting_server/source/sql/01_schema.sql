-- OLTP schema (3NF) for the simulated Olist shop. DB name comes from MYSQL_DATABASE.
-- created_at / updated_at are wall-clock (system) times; business timestamps keep CSV values.

CREATE TABLE customers (
  customer_id VARCHAR(32) PRIMARY KEY,
  customer_unique_id VARCHAR(32) NOT NULL,
  customer_zip_code_prefix CHAR(5),
  customer_city VARCHAR(64),
  customer_state CHAR(2),
  created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  KEY idx_upd (updated_at),
  KEY idx_unique (customer_unique_id)
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
  KEY idx_customer (customer_id),
  CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE order_items (
  order_id VARCHAR(32) NOT NULL,
  order_item_id INT NOT NULL,
  product_id VARCHAR(32),
  seller_id VARCHAR(32),
  shipping_limit_date DATETIME,
  price DECIMAL(10,2),
  freight_value DECIMAL(10,2),
  created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (order_id, order_item_id),
  KEY idx_upd (updated_at)
);

CREATE TABLE order_payments (
  order_id VARCHAR(32) NOT NULL,
  payment_sequential INT NOT NULL,
  payment_type VARCHAR(16),
  payment_installments INT,
  payment_value DECIMAL(10,2),
  created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (order_id, payment_sequential),
  KEY idx_upd (updated_at)
);

-- review_id is NOT unique in the CSV (814 duplicate rows) -> surrogate key
CREATE TABLE order_reviews (
  review_pk BIGINT AUTO_INCREMENT PRIMARY KEY,
  review_id VARCHAR(32),
  order_id VARCHAR(32) NOT NULL,
  review_score TINYINT,
  review_comment_title VARCHAR(255),
  review_comment_message TEXT,
  review_creation_date DATETIME,
  review_answer_timestamp DATETIME,
  created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_review (review_id, order_id),
  KEY idx_upd (updated_at),
  KEY idx_order (order_id)
);

-- Reference tables (seeded once, snapshotted daily by Airflow)
CREATE TABLE products (
  product_id VARCHAR(32) PRIMARY KEY,
  product_category_name VARCHAR(64),
  product_name_lenght INT,
  product_description_lenght INT,
  product_photos_qty INT,
  product_weight_g INT,
  product_length_cm INT,
  product_height_cm INT,
  product_width_cm INT
);

CREATE TABLE sellers (
  seller_id VARCHAR(32) PRIMARY KEY,
  seller_zip_code_prefix CHAR(5),
  seller_city VARCHAR(64),
  seller_state CHAR(2)
);

CREATE TABLE geolocation (
  geo_pk BIGINT AUTO_INCREMENT PRIMARY KEY,
  geolocation_zip_code_prefix CHAR(5),
  geolocation_lat DOUBLE,
  geolocation_lng DOUBLE,
  geolocation_city VARCHAR(64),
  geolocation_state CHAR(2),
  KEY idx_zip (geolocation_zip_code_prefix)
);

CREATE TABLE product_category_name_translation (
  product_category_name VARCHAR(64) PRIMARY KEY,
  product_category_name_english VARCHAR(64)
);

-- Simulator control
CREATE TABLE sim_state (
  id TINYINT PRIMARY KEY DEFAULT 1,
  virtual_ts DATETIME NOT NULL,
  speed DOUBLE NOT NULL,
  status VARCHAR(8) NOT NULL DEFAULT 'running',
  updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
);
