-- Customers Table
CREATE TABLE IF NOT EXISTS `chetan-retailer-project-454413.silver.customers` (
    customer_id INT64,
    name STRING,
    email STRING,
    updated_at STRING,
    is_quarantined BOOL,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOL
);

WITH source AS (
    SELECT DISTINCT *,
        CASE WHEN customer_id IS NULL OR email IS NULL OR name IS NULL THEN TRUE ELSE FALSE END AS is_quarantined,
        CURRENT_TIMESTAMP() AS effective_start_date,
        CURRENT_TIMESTAMP() AS effective_end_date,
        TRUE AS is_active
    FROM `chetan-retailer-project-454413.bronze.customers`
)
MERGE INTO `chetan-retailer-project-454413.silver.customers` target
USING source
ON target.customer_id = source.customer_id AND target.is_active = TRUE
WHEN MATCHED AND (target.name != source.name OR target.email != source.email OR target.updated_at != source.updated_at)
THEN UPDATE SET target.is_active = FALSE, target.effective_end_date = CURRENT_TIMESTAMP();

MERGE INTO `chetan-retailer-project-454413.silver.customers` target
USING source
ON target.customer_id = source.customer_id AND target.is_active = TRUE
WHEN NOT MATCHED THEN 
    INSERT (customer_id, name, email, updated_at, is_quarantined, effective_start_date, effective_end_date, is_active)
    VALUES (source.customer_id, source.name, source.email, source.updated_at, source.is_quarantined, source.effective_start_date, source.effective_end_date, source.is_active);

-- Orders Table
CREATE TABLE IF NOT EXISTS `chetan-retailer-project-454413.silver.orders` (
    order_id INT64,
    customer_id INT64,
    order_date STRING,
    total_amount FLOAT64,
    updated_at STRING,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOL
);

WITH source AS (
    SELECT DISTINCT *, CURRENT_TIMESTAMP() AS effective_start_date, CURRENT_TIMESTAMP() AS effective_end_date, TRUE AS is_active
    FROM `chetan-retailer-project-454413.bronze.orders`
)
MERGE INTO `chetan-retailer-project-454413.silver.orders` target
USING source
ON target.order_id = source.order_id AND target.is_active = TRUE
WHEN MATCHED AND (target.customer_id != source.customer_id OR target.order_date != source.order_date OR target.total_amount != source.total_amount OR target.updated_at != source.updated_at)
THEN UPDATE SET target.is_active = FALSE, target.effective_end_date = CURRENT_TIMESTAMP();

MERGE INTO `chetan-retailer-project-454413.silver.orders` target
USING source
ON target.order_id = source.order_id AND target.is_active = TRUE
WHEN NOT MATCHED THEN 
    INSERT (order_id, customer_id, order_date, total_amount, updated_at, effective_start_date, effective_end_date, is_active)
    VALUES (source.order_id, source.customer_id, source.order_date, source.total_amount, source.updated_at, source.effective_start_date, source.effective_end_date, source.is_active);

-- Categories Table
CREATE TABLE IF NOT EXISTS `chetan-retailer-project-454413.silver.categories` (
    category_id INT64,
    name STRING,
    updated_at STRING,
    is_quarantined BOOL
);

TRUNCATE TABLE `chetan-retailer-project-454413.silver.categories`;

INSERT INTO `chetan-retailer-project-454413.silver.categories`
SELECT *, CASE WHEN category_id IS NULL OR name IS NULL THEN TRUE ELSE FALSE END AS is_quarantined
FROM `chetan-retailer-project-454413.bronze.categories`;

-- Products Table
CREATE TABLE IF NOT EXISTS `chetan-retailer-project-454413.silver.products` (
  product_id INT64,
  name STRING,
  category_id INT64,
  price FLOAT64,
  updated_at STRING,
  is_quarantined BOOL
);

TRUNCATE TABLE `chetan-retailer-project-454413.silver.products`;

INSERT INTO `chetan-retailer-project-454413.silver.products`
SELECT *, CASE WHEN category_id IS NULL OR name IS NULL THEN TRUE ELSE FALSE END AS is_quarantined
FROM `chetan-retailer-project-454413.bronze.products`;

-- Suppliers Table
CREATE TABLE IF NOT EXISTS `chetan-retailer-project-454413.silver.suppliers` (
  supplier_id INT64,
  supplier_name STRING,
  contact_name STRING,
  phone STRING,
  email STRING,
  address STRING,
  city STRING,
  country STRING,
  created_at STRING,
  is_quarantined BOOL
);

TRUNCATE TABLE `chetan-retailer-project-454413.silver.suppliers`;

INSERT INTO `chetan-retailer-project-454413.silver.suppliers`
SELECT *, CASE WHEN supplier_id IS NULL OR supplier_name IS NULL THEN TRUE ELSE FALSE END AS is_quarantined
FROM `chetan-retailer-project-454413.bronze.suppliers`;
