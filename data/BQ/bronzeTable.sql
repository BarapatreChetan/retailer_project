CREATE EXTERNAL TABLE IF NOT EXISTS `chetan-retailer-project-454413.bronze.orders`(
    order_id INT64,
    customer_id INT64,
    order_date STRING,
    total_amount FLOAT64,
    updated_at STRING
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://chetan-retailer-deltalake-project/landing/retailer-db/orders/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `chetan-retailer-project-454413.bronze.customers`
(
    customer_id INT64,
    name STRING,
    email STRING,
    updated_at STRING
)
OPTIONS (
    format = 'JSON',
    uris = ['gs://chetan-retailer-deltalake-project/landing/retailer-db/customers/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `chetan-retailer-project-454413.bronze.products`
(
    product_id INT64,
    name STRING,
    category_id INT64,
    price FLOAT64,
    updated_at STRING
)
OPTIONS (
    format = 'JSON',
    uris = ['gs://chetan-retailer-deltalake-project/landing/retailer-db/products/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `chetan-retailer-project-454413.bronze.categories`
(
    category_id INT64,
    name STRING,
    updated_at STRING
)
OPTIONS (
    format = 'JSON',
    uris = ['gs://chetan-retailer-deltalake-project/landing/retailer-db/categories/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `chetan-retailer-project-454413.bronze.order_items`
(
    order_item_id INT64,
    order_id INT64,
    product_id INT64,
    quantity INT64,
    price FLOAT64,
    updated_at STRING
)
OPTIONS (
    format = 'JSON',
    uris = ['gs://chetan-retailer-deltalake-project/landing/retailer-db/order_items/*.json']
);
-------------------------------------------------------------------------------------------------------------
-- Suppliers Table
CREATE EXTERNAL TABLE IF NOT EXISTS `chetan-retailer-project-454413.bronze.suppliers` (
    supplier_id INT64,
    supplier_name STRING,
    contact_name STRING,
    phone STRING,
    email STRING,
    address STRING,
    city STRING,
    country STRING,
    created_at STRING
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://chetan-retailer-deltalake-project/landing/supplier-db/suppliers/*.json']
);

-- Product Suppliers Table (Mapping suppliers to products)
CREATE EXTERNAL TABLE IF NOT EXISTS `chetan-retailer-project-454413.bronze.product_suppliers` (
    supplier_id INT64,
    product_id INT64,
    supply_price FLOAT64,
    last_updated STRING
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://chetan-retailer-deltalake-project/landing/supplier-db/product_suppliers/*.json']
);

-------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE EXTERNAL TABLE `chetan-retailer-project-454413.bronze.customer_reviews` (
  id STRING,
  customer_id INT64,
  product_id INT64,
  rating INT64,
  review_text STRING,
  review_date STRING
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://chetan-retailer-deltalake-project/landing/customer_reviews/customer_reviews_*.parquet']
);

-------------------------------------------------------------------------------------------------------------