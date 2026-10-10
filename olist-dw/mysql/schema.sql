CREATE DATABASE IF NOT EXISTS olist_dw
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE olist_dw;

SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS fact_customer_order;
DROP TABLE IF EXISTS fact_reviews;
DROP TABLE IF EXISTS fact_payments;
DROP TABLE IF EXISTS fact_order_items;
DROP TABLE IF EXISTS fact_orders;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_seller;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_category;
DROP TABLE IF EXISTS dim_geolocation;
DROP TABLE IF EXISTS dim_date;

SET FOREIGN_KEY_CHECKS = 1;

CREATE TABLE dim_geolocation (
    geolocation_key BIGINT NOT NULL,
    zip_code_prefix INT NOT NULL,
    city VARCHAR(120),
    state VARCHAR(10),
    latitude DOUBLE,
    longitude DOUBLE,
    source_row_count BIGINT,
    PRIMARY KEY (geolocation_key),
    UNIQUE KEY uq_geo_zip (zip_code_prefix)
) ENGINE=InnoDB;

CREATE TABLE dim_category (
    category_key BIGINT NOT NULL,
    product_category_name VARCHAR(150) NOT NULL,
    product_category_name_english VARCHAR(150),
    PRIMARY KEY (category_key),
    UNIQUE KEY uq_category_name (product_category_name)
) ENGINE=InnoDB;

CREATE TABLE dim_customer (
    customer_key BIGINT NOT NULL,
    customer_unique_id VARCHAR(64) NOT NULL,
    city VARCHAR(120),
    state VARCHAR(10),
    geolocation_key BIGINT,
    PRIMARY KEY (customer_key),
    UNIQUE KEY uq_customer_unique_id (customer_unique_id),
    CONSTRAINT fk_customer_geo
        FOREIGN KEY (geolocation_key) REFERENCES dim_geolocation(geolocation_key)
) ENGINE=InnoDB;

CREATE TABLE dim_seller (
    seller_key BIGINT NOT NULL,
    seller_id VARCHAR(64) NOT NULL,
    city VARCHAR(120),
    state VARCHAR(10),
    geolocation_key BIGINT,
    PRIMARY KEY (seller_key),
    UNIQUE KEY uq_seller_id (seller_id),
    CONSTRAINT fk_seller_geo
        FOREIGN KEY (geolocation_key) REFERENCES dim_geolocation(geolocation_key)
) ENGINE=InnoDB;

CREATE TABLE dim_product (
    product_key BIGINT NOT NULL,
    product_id VARCHAR(64) NOT NULL,
    category_key BIGINT,
    product_category_name VARCHAR(150),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g DOUBLE,
    product_length_cm DOUBLE,
    product_height_cm DOUBLE,
    product_width_cm DOUBLE,
    PRIMARY KEY (product_key),
    UNIQUE KEY uq_product_id (product_id),
    CONSTRAINT fk_product_category
        FOREIGN KEY (category_key) REFERENCES dim_category(category_key)
) ENGINE=InnoDB;

CREATE TABLE dim_date (
    date_key INT NOT NULL,
    date DATE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    week_of_year INT,
    day INT,
    day_of_week INT,
    is_weekend BOOLEAN,
    PRIMARY KEY (date_key),
    UNIQUE KEY uq_date (date)
) ENGINE=InnoDB;

CREATE TABLE fact_orders (
    order_key BIGINT NOT NULL,
    order_id VARCHAR(64) NOT NULL,
    customer_key BIGINT NOT NULL,
    purchase_date_key INT NOT NULL,
    order_status VARCHAR(30),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,
    delivery_days INT,
    estimated_delivery_days INT,
    delivery_delay_days INT,
    is_delivered TINYINT,
    is_canceled TINYINT,
    PRIMARY KEY (order_key),
    UNIQUE KEY uq_order_id (order_id),
    KEY idx_orders_customer (customer_key),
    KEY idx_orders_date (purchase_date_key),
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    CONSTRAINT fk_orders_date
        FOREIGN KEY (purchase_date_key) REFERENCES dim_date(date_key)
) ENGINE=InnoDB;

CREATE TABLE fact_order_items (
    item_key BIGINT NOT NULL,
    order_key BIGINT NOT NULL,
    order_id VARCHAR(64) NOT NULL,
    order_item_id INT NOT NULL,
    product_key BIGINT NOT NULL,
    seller_key BIGINT NOT NULL,
    date_key INT,
    shipping_limit_date DATETIME,
    price DOUBLE,
    freight_value DOUBLE,
    item_total DOUBLE,
    PRIMARY KEY (item_key),
    UNIQUE KEY uq_order_item (order_id, order_item_id),
    KEY idx_items_order (order_key),
    KEY idx_items_product (product_key),
    KEY idx_items_seller (seller_key),
    KEY idx_items_date (date_key),
    CONSTRAINT fk_items_order
        FOREIGN KEY (order_key) REFERENCES fact_orders(order_key),
    CONSTRAINT fk_items_product
        FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    CONSTRAINT fk_items_seller
        FOREIGN KEY (seller_key) REFERENCES dim_seller(seller_key),
    CONSTRAINT fk_items_date
        FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
) ENGINE=InnoDB;

CREATE TABLE fact_payments (
    payment_key BIGINT NOT NULL,
    order_key BIGINT NOT NULL,
    order_id VARCHAR(64) NOT NULL,
    payment_sequential INT NOT NULL,
    payment_type VARCHAR(40),
    payment_installments INT,
    payment_value DOUBLE,
    PRIMARY KEY (payment_key),
    UNIQUE KEY uq_payment (order_id, payment_sequential),
    KEY idx_payments_order (order_key),
    CONSTRAINT fk_payments_order
        FOREIGN KEY (order_key) REFERENCES fact_orders(order_key)
) ENGINE=InnoDB;

CREATE TABLE fact_reviews (
    review_key BIGINT NOT NULL,
    order_key BIGINT NOT NULL,
    order_id VARCHAR(64) NOT NULL,
    review_id VARCHAR(64) NOT NULL,
    review_score INT,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date DATETIME,
    review_answer_timestamp DATETIME,
    is_positive TINYINT,
    is_negative TINYINT,
    PRIMARY KEY (review_key),
    UNIQUE KEY uq_review_id (review_id),
    KEY idx_reviews_order (order_key),
    CONSTRAINT fk_reviews_order
        FOREIGN KEY (order_key) REFERENCES fact_orders(order_key)
) ENGINE=InnoDB;

CREATE TABLE fact_customer_order (
    customer_order_key BIGINT NOT NULL,
    customer_key BIGINT NOT NULL,
    order_count BIGINT,
    delivered_order_count BIGINT,
    canceled_order_count BIGINT,
    total_spent DOUBLE,
    average_order_value DOUBLE,
    first_order_timestamp DATETIME,
    last_order_timestamp DATETIME,
    average_delivery_days DOUBLE,
    PRIMARY KEY (customer_order_key),
    UNIQUE KEY uq_customer_order_customer (customer_key),
    CONSTRAINT fk_customer_order_customer
        FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key)
) ENGINE=InnoDB;
