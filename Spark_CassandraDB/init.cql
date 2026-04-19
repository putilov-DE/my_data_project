-- Create the Keyspace (Database)
CREATE KEYSPACE IF NOT EXISTS amazon
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE amazon;

-- 1. Reviews by product
CREATE TABLE IF NOT EXISTS reviews_by_product (
    product_id text,
    review_date date,
    review_id text,
    customer_id text,
    star_rating int,
    review_headline text,
    review_body text,
    PRIMARY KEY ((product_id), review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC, review_id ASC);

-- 2. Reviews by product and rating (Composite Partition Key)
CREATE TABLE IF NOT EXISTS reviews_by_product_rating (
    product_id text,
    star_rating int,
    review_date date,
    review_id text,
    customer_id text,
    review_headline text,
    review_body text,
    PRIMARY KEY ((product_id, star_rating), review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC, review_id ASC);

-- 3. Reviews by customer
CREATE TABLE IF NOT EXISTS reviews_by_customer (
    customer_id text,
    review_date date,
    review_id text,
    product_id text,
    star_rating int,
    review_headline text,
    PRIMARY KEY ((customer_id), review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC, review_id ASC);

-- 4. Top products by period (Data Mart)
CREATE TABLE IF NOT EXISTS top_products_by_period (
    period text,
    review_count int,
    product_id text,
    PRIMARY KEY ((period), review_count, product_id)
) WITH CLUSTERING ORDER BY (review_count DESC, product_id ASC);

-- 5. Top productive customers (Verified Purchases only)
CREATE TABLE IF NOT EXISTS top_customers_verified (
    period text,
    review_count int,
    customer_id text,
    PRIMARY KEY ((period), review_count, customer_id)
) WITH CLUSTERING ORDER BY (review_count DESC, customer_id ASC);

-- 6. Top haters (1-2 stars)
CREATE TABLE IF NOT EXISTS top_haters (
    period text,
    review_count int,
    customer_id text,
    PRIMARY KEY ((period), review_count, customer_id)
) WITH CLUSTERING ORDER BY (review_count DESC, customer_id ASC);

-- 7. Top backers (4-5 stars)
CREATE TABLE IF NOT EXISTS top_backers (
    period text,
    review_count int,
    customer_id text,
    PRIMARY KEY ((period), review_count, customer_id)
) WITH CLUSTERING ORDER BY (review_count DESC, customer_id ASC);