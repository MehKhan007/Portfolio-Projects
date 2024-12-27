

------------------------------------- -- DATA CLEANING -- ------------------------------------

CREATE TABLE ecom_dataset (
    product_id VARCHAR(20),         -- Product ID as a unique identifier
    product_name VARCHAR(255),         -- Product name
    category VARCHAR(100),                      -- Category of the product
    price DECIMAL(10, 2),              -- Price of the product
    discount DECIMAL(5, 2),                     -- Discount percentage
    tax_rate DECIMAL(5, 2),                     -- Tax rate percentage
    stock_level INT,                            -- Stock quantity available
    supplier_id VARCHAR(20),                    -- Identifier for the supplier
    customer_age_group VARCHAR(20),             -- Age group of the customer
    customer_location VARCHAR(255),             -- Location of the customer
    customer_gender VARCHAR(50),                -- Gender of the customer
    shipping_cost DECIMAL(10, 2),               -- Shipping cost
    shipping_method VARCHAR(50),                -- Shipping method
    return_rate DECIMAL(5, 2),                  -- Return rate percentage
    seasonality ENUM('Yes', 'No'),              -- Whether the product has seasonal demand
    popularity_index INT                        -- Popularity index of the product
);

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Data\\diversified_ecommerce_dataset.csv'
INTO TABLE ecom_dataset
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

select count(*) AS total_rows
FROM ecom_dataset;


-- Remove Duplicates

select * from ecom_dataset;

CREATE TABLE ecom_dataset_stagings
LIKE ecom_dataset;

select * from ecom_dataset_stagings;

INSERT ecom_dataset_stagings
SELECT *
FROM ecom_dataset;

select * from ecom_dataset_stagings;

select count(*) AS total_rows
FROM ecom_dataset;

SELECT *,
ROW_NUMBER () OVER (PARTITION BY product_id,product_name,category,price,discount,tax_rate,stock_level
,supplier_id,customer_age_group,customer_location,customer_gender,shipping_cost
,shipping_method,return_rate,seasonality,popularity_index)
AS row_num FROM ecom_dataset_stagings;

SELECT product_id,product_name,category,price,discount,tax_rate,stock_level
,supplier_id,customer_age_group,customer_location,customer_gender,shipping_cost
,shipping_method,return_rate,seasonality,popularity_index, COUNT(*) AS count
FROM ecom_dataset_stagings
GROUP BY product_id,product_name,category,price,discount,tax_rate,stock_level
,supplier_id,customer_age_group,customer_location,customer_gender,shipping_cost
,shipping_method,return_rate,seasonality,popularity_index
HAVING COUNT(*) > 1;

WITH function_as AS
(SELECT *,
ROW_NUMBER () OVER (PARTITION BY product_id,product_name,category,price,discount,tax_rate,stock_level
,supplier_id,customer_age_group,customer_location,customer_gender,shipping_cost
,shipping_method,return_rate,seasonality,popularity_index)
AS row_num FROM ecom_dataset_stagings)
SELECT * 
FROM function_as
WHERE row_num > 1;

CREATE TABLE `ecom_dataset2` (
  `product_id` varchar(20) DEFAULT NULL,
  `product_name` varchar(255) DEFAULT NULL,
  `category` varchar(100) DEFAULT NULL,
  `price` decimal(10,2) DEFAULT NULL,
  `discount` decimal(5,2) DEFAULT NULL,
  `tax_rate` decimal(5,2) DEFAULT NULL,
  `stock_level` int DEFAULT NULL,
  `supplier_id` varchar(20) DEFAULT NULL,
  `customer_age_group` varchar(20) DEFAULT NULL,
  `customer_location` varchar(255) DEFAULT NULL,
  `customer_gender` varchar(50) DEFAULT NULL,
  `shipping_cost` decimal(10,2) DEFAULT NULL,
  `shipping_method` varchar(50) DEFAULT NULL,
  `return_rate` decimal(5,2) DEFAULT NULL,
  `seasonality` enum('Yes','No') DEFAULT NULL,
  `popularity_index` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM ecom_dataset2;

INSERT INTO ecom_dataset2
SELECT *,
ROW_NUMBER () OVER(
PARTITION BY product_id,product_name,category,price,discount,tax_rate,stock_level
,supplier_id,customer_age_group,customer_location,customer_gender,shipping_cost
,shipping_method,return_rate,seasonality,popularity_index) AS row_num
FROM ecom_dataset_stagings;

SELECT *
FROM ecom_dataset2;

SELECT *
FROM ecom_dataset2
WHERE row_num > 1;

SELECT *
FROM ecom_dataset2
WHERE row_num > 1;
                                                                     -- NO DUPLICATE VALUES EXIST IN THE DATASET --

-- Standardize the Data

SELECT *
FROM ecom_dataset2;

-- IN CASE THERE IS ANY SPACE BEFORE THE TEXT IN THE ROWS --

SELECT product_name, TRIM(product_name)
FROM ecom_dataset2;

UPDATE ecom_dataset2
SET product_name = TRIM(product_name);

SELECT *
FROM ecom_dataset2;

SELECT DISTINCT category
FROM ecom_dataset2
ORDER BY 1;

SELECT *
FROM ecom_dataset2;

SELECT DISTINCT customer_location
FROM ecom_dataset2;

SELECT *
FROM ecom_dataset2;


-- Null Values or Blank Values

SELECT COUNT(*) AS null_rows
FROM ecom_dataset2
WHERE 
    product_id IS NULL 
    OR product_name IS NULL 
    OR category IS NULL 
    OR price IS NULL 
    OR discount IS NULL 
    OR tax_rate IS NULL  
    OR stock_level IS NULL 
    OR supplier_id IS NULL 
    OR customer_age_group IS NULL 
    OR customer_location IS NULL 
    OR customer_gender IS NULL 
    OR shipping_cost IS NULL 
    OR shipping_method IS NULL 
    OR return_rate IS NULL 
    OR seasonality IS NULL 
    OR popularity_index IS NULL;


SELECT *
FROM ecom_dataset2
WHERE 
    product_id IS NULL 
    AND product_name IS NULL 
    AND category IS NULL 
    AND price IS NULL 
    AND discount IS NULL 
    AND tax_rate IS NULL  
    AND stock_level IS NULL 
    AND supplier_id IS NULL 
    AND customer_age_group IS NULL 
    AND customer_location IS NULL 
    AND customer_gender IS NULL 
    AND shipping_cost IS NULL 
    AND shipping_method IS NULL 
    AND return_rate IS NULL 
    AND seasonality IS NULL 
    AND popularity_index IS NULL;
    
    SELECT 
    SUM(product_id IS NULL OR TRIM(product_id) = '' OR LENGTH(product_id) > 20) AS product_id_issues,
    SUM(product_name IS NULL OR TRIM(product_name) = '') AS product_name_issues,
    SUM(category IS NULL OR TRIM(category) = '' OR LENGTH(category) > 100) AS category_issues,
    SUM(price IS NULL OR price <= 0) AS price_issues,
    SUM(discount IS NULL OR discount < 0 OR discount > 100) AS discount_issues,
    SUM(tax_rate IS NULL OR tax_rate < 0 OR tax_rate > 100) AS tax_rate_issues,
    SUM(stock_level IS NULL OR stock_level < 0) AS stock_level_issues,
    SUM(supplier_id IS NULL OR TRIM(supplier_id) = '' OR LENGTH(supplier_id) > 20) AS supplier_id_issues,
    SUM(customer_age_group IS NULL OR TRIM(customer_age_group) = '') AS customer_age_group_issues,
    SUM(customer_location IS NULL OR TRIM(customer_location) = '') AS customer_location_issues,
    SUM(customer_gender IS NULL OR NOT customer_gender IN ('Male', 'Female', 'Non-Binary')) AS customer_gender_issues,
    SUM(shipping_cost IS NULL OR shipping_cost < 0) AS shipping_cost_issues,
    SUM(shipping_method IS NULL OR TRIM(shipping_method) = '') AS shipping_method_issues,
    SUM(return_rate IS NULL OR return_rate < 0 OR return_rate > 100) AS return_rate_issues,
    SUM(seasonality IS NULL OR NOT seasonality IN ('Yes', 'No')) AS seasonality_issues,
    SUM(popularity_index IS NULL OR popularity_index < 0 OR popularity_index > 100) AS popularity_index_issues
FROM ecom_dataset2;

-- ------------------------------------------------- NO NULL VALUES ----------------------------------------------
    
SELECT *
FROM ecom_dataset2;


-- Remove any Columns or Rows

SELECT *
FROM ecom_dataset2;

ALTER TABLE ecom_dataset2
DROP COLUMN row_num;

SELECT *
FROM ecom_dataset2;


-- -------------------- ------------------------------ -- **EXPLORATORY DATA ANALYSIS (EDA)** -- ------------------------------------------------------

SELECT * 
FROM ecom_dataset2;

SELECT product_name
FROM ecom_dataset2
ORDER BY product_name DESC
LIMIT 1;


SELECT MAX(product_name) as MOST_SELLING_PRODUCT
FROM ecom_dataset2;

-- WASHING MACHINE IS THE MOST SELLING PRODUCT --

SELECT * 
FROM ecom_dataset2;

SELECT DISTINCT(product_name), price, customer_location, customer_age_group, customer_gender
FROM ecom_dataset2
ORDER BY price DESC;

-- HIGHEST PRICE RECORDED WAS FOR SHIRT IN PARIS, FRANCE, WITH CUSTOMER AGE GROUP OF 25-34 AND GENDER BEING NON-BINARY --

SELECT * 
FROM ecom_dataset2;

SELECT MAX(category) as MOST_SELLING_CATEGORY
FROM ecom_dataset2;

SELECT category
FROM ecom_dataset2
ORDER BY category DESC
LIMIT 1;

-- HOME APPLIANCES IS THE MOST SELLING CATEGORY --

SELECT * 
FROM ecom_dataset2;

SELECT 
    product_id,
    product_name,
    price,
    discount,
    tax_rate,
 (price - (price * discount / 100)) + ((price - (price * discount / 100)) * tax_rate / 100) AS net_price
FROM ecom_dataset2;

-- ADDING NEW COLUMN NET_PRICE --

ALTER TABLE ecom_dataset2
ADD COLUMN net_price DECIMAL (10, 2) AFTER tax_rate;

SELECT * 
FROM ecom_dataset2;

UPDATE ecom_dataset2
SET net_price = (price - (price * discount / 100)) + ((price - (price * discount / 100)) * tax_rate / 100);

-- IN CASE LOCK WAIT TIMEOUT EXCEEDS (SPECIFIC THIS TIME ONLY) --
SHOW PROCESSLIST;
KILL 8;

SELECT * 
FROM ecom_dataset2;

SELECT product_name, category, MAX(net_price)
FROM ecom_dataset2
GROUP BY product_name, category;

-- HIGHEST NET PRICE IN THIS DATASET IS FOR COOKBOOKS IN THE CATEGORY OF BOOKS --

SELECT * 
FROM ecom_dataset2;

SELECT DISTINCT(product_name), category, min(stock_level) as min_stock_levels, MAX(net_price) AS max_net_price
FROM ecom_dataset2
WHERE net_price > 2000
GROUP BY product_name, category
ORDER BY max_net_price DESC, min_stock_levels ASC LIMIT 5;

-- MIN STOCK LEVELS INDICATE BETTER INVENTORY MANAGEMENT AND PROFIT IN TERMS OF PRODUCT SALES --
-- THE TOP FIVE PRODUCTS WITH LEAST STOCK LEVELS WERE --
-- 1 COOKBOOKS --
-- 2 FICTION --
-- 3 GRAPHIC NOVELS --
-- 4 SMARTPHONE --
-- 5 DISWASHER --

-- WE CAN CONCLUDE THAT THE BOOKS USUALLY SELL BETTER THAN THE OVERALL PRODUCTS CONSIDERING THE NET_PRICE AND MINIMUM STOCK LEVELS --

SELECT * 
FROM ecom_dataset2;

SELECT customer_age_group, COUNT(*) AS total_orders
FROM ecom_dataset2
GROUP BY customer_age_group
ORDER BY total_orders DESC;

-- FROM THIS WE CAN CONCLUDE THAT MOST ORDERS WERE FROM THE CUSTOMERS OF THE AGE GROUP (18-24) --

SELECT * 
FROM ecom_dataset2;

SELECT customer_location, COUNT(*) AS total_orders
FROM ecom_dataset2
GROUP BY customer_location
ORDER BY total_orders DESC;

-- MOST OF THE ORDERS WERE FROM DUBAI, UAE --

SELECT * 
FROM ecom_dataset2;

SELECT product_name, category, customer_gender, COUNT(*) as total_orders
FROM ecom_dataset2
WHERE customer_gender = 'Male'
GROUP BY product_name, category, customer_gender
ORDER BY total_orders DESC LIMIT 2;

-- MOST ORDERS WERE FOR THE PRODUCTS OF TEXTBOOKS AND FICTION FOR THE GENDER "MALE" --

SELECT product_name, category, customer_gender, COUNT(*) as total_orders
FROM ecom_dataset2
WHERE customer_gender = 'Female'
GROUP BY product_name, category, customer_gender
ORDER BY total_orders DESC LIMIT 2;

-- MOST ORDERS WERE FOR THE PRODUCTS OF COMICS AND GRAPHIC NOVELS FOR THE GENDER "FEMALE" --

SELECT product_name, category, customer_gender, COUNT(*) as total_orders
FROM ecom_dataset2
WHERE customer_gender = 'Non-Binary'
GROUP BY product_name, category, customer_gender
ORDER BY total_orders DESC LIMIT 2;

-- MOST ORDERS WERE FOR THE PRODUCTS OF GRAPHIC NOVELS AND VACUMM CLEANER FOR THE GENDER "NON-BINARY" --

SELECT * 
FROM ecom_dataset2;

WITH cte AS
(SELECT product_name, category,
AVG(shipping_cost) AS average_shipping_cost,
MIN(shipping_cost) AS minimum_shipping_cost,
max(shipping_cost) AS maximum_shipping_cost
FROM ecom_dataset2
GROUP BY product_name, category)
SELECT 
    MIN(average_shipping_cost) AS "Minimum Average Shipping Cost",
    MAX(average_shipping_cost) AS "Maximum Average Shipping Cost"
FROM cte;

-- Minimum Average Shipping Cost is 24.77 --
-- Maximum Average Shipping Cost is 25.13 --

SELECT * 
FROM ecom_dataset2;

SELECT shipping_method, count(*) as "Most Common Shipping Method"
FROM ecom_dataset2
GROUP BY shipping_method
ORDER by "Most Common Shipping Method" LIMIT 1 ;

-- Most Common Shipping Method is the STANDARD SHIPPING METHOD --

SELECT * 
FROM ecom_dataset2;

WITH CTE AS
(SELECT 
DISTINCT(product_name),
category,
return_rate
FROM ecom_dataset2)

SELECT *
FROM CTE
WHERE return_rate = (SELECT MAX(return_rate) FROM CTE)
OR return_rate = (SELECT MIN(return_rate) FROM CTE);


SELECT product_name, 
       category, 
       return_rate
FROM ecom_dataset2
WHERE return_rate = (SELECT MAX(return_rate) FROM ecom_dataset2);

-- MAXIMUM RETURN RARE IS 20% --

SELECT product_name, 
       category, 
       return_rate
FROM ecom_dataset2
WHERE return_rate = (SELECT MIN(return_rate) FROM ecom_dataset2);

-- MINIMUM RETURN RATE IS 1% --

SELECT * 
FROM ecom_dataset2;

SELECT product_name, 
       category, 
       MAX(popularity_index) AS highest_popularity_index,
       SUM(stock_level) AS total_stock,
       AVG(net_price) AS average_net_price
FROM ecom_dataset2
GROUP BY product_name, category
ORDER BY highest_popularity_index DESC, total_stock DESC, average_net_price DESC
LIMIT 5;


-- TOP FIVE PRODUCTS WITH HIGHEST POPULARITY INDEX WERE --
-- 1 GRAPHIC NOVELS --
-- 2 VACUMM CLEANER --
-- 3 TEXTBOOKS --
-- 4 BIOGRAPHIES --
-- 5 FICTION --

SELECT product_name, 
       category, 
       MIN(popularity_index) AS lowest_popularity_index,
       SUM(stock_level) AS total_stock,
       AVG(net_price) AS average_net_price
FROM ecom_dataset2
GROUP BY product_name, category
ORDER BY lowest_popularity_index ASC, total_stock ASC, average_net_price ASC
LIMIT 5;


-- TOP FIVE PRODUCTS WITH LOWEST POPULARITY INDEX WERE --
-- 1 HIKING SHOES --
-- 2 DRESS --
-- 3 MONITOR --
-- 4 FLATS --
-- 5 JACKET --



SELECT * FROM (
    SELECT 'Query 1' AS query_label, 
           product_name, 
           category, 
           MAX(popularity_index) AS popularity_index,
           SUM(stock_level) AS total_stock,
           AVG(net_price) AS average_net_price
    FROM ecom_dataset2
    GROUP BY product_name, category
    ORDER BY popularity_index DESC, total_stock DESC, average_net_price DESC
    LIMIT 5
) AS q1

UNION ALL

SELECT * FROM (
    SELECT 'Query 2' AS query_label, 
           product_name, 
           category, 
           MIN(popularity_index) AS popularity_index,
           SUM(stock_level) AS total_stock,
           AVG(net_price) AS average_net_price
    FROM ecom_dataset2
    GROUP BY product_name, category
    ORDER BY popularity_index ASC, total_stock ASC, average_net_price ASC
    LIMIT 5
) AS q2;
