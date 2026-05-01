-- Create Keyspace with local replication
CREATE KEYSPACE IF NOT EXISTS wiki_space
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE wiki_space;

-- Create Results Table
-- 'domain' is the Partition Key to distribute data across nodes
-- 'created_at' and 'page_title' are Clustering Keys for sorting and uniqueness
CREATE TABLE IF NOT EXISTS processed_pages (
    domain text,
    created_at timestamp,
    user_id text,
    page_title text,
    PRIMARY KEY ((domain), created_at, page_title)
) WITH CLUSTERING ORDER BY (created_at DESC);