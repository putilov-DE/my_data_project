# AdTech Data Pipeline: ETL & NoSQL Analytics

## Project Overview
This project is an end-to-end ETL (Extract, Transform, Load) pipeline built for processing advertising technology (AdTech) data. The primary goal is to ingest raw data from multiple sources, clean and denormalize it, and load it into a high-performance NoSQL database (Apache Cassandra) to enable lightning-fast analytical queries.

## Tech Stack
* Language: Python
* Data Processing: Pandas, NumPy
* Database: Apache Cassandra (deployed via Docker)
* Driver: "cassandra-driver" (using "BatchStatement" and "execute_concurrent_with_args")
* Environment: Jupyter Notebook / PyCharm

## ETL Architecture

### 1. Extract
Data is ingested from three separate, raw CSV files:
* "events_header.csv": Logs of ad impressions and clicks.
* "users_header.csv": User demographics and geographical data.
* "campaigns_header.csv": Advertising campaign budgets and metadata.

### 2. Transform
During this stage, the raw data is cleaned and reshaped into a query-ready dataset:
* Data Denormalization: Merging relational tables based on exact keys ("UserID", "CampaignName") into a flat structure required for NoSQL efficiency.
* Data Cleansing: Resolving case-sensitivity issues and standardizing column names (e.g., mapping "Location" from users to "region", standardizing "UserID" and "AdCost").
* Feature Engineering: Creating boolean flags for click events based on mixed data types ("WasClicked") and parsing string timestamps into datetime objects for proper Cassandra clustering.
* Downsampling: Limiting the dataset to 1,000,000 rows to ensure stable local development and Docker container performance.

### 3. Load (Query-Driven Design)
Following Cassandra's best practices, the database schema was built around the "One Query = One Table" philosophy. Five materialized views were populated:
1. "ctr_per_campaign": Daily Click-Through Rate (CTR) calculations.
2. "top_advertisers_spend": Top 5 advertisers based on total ad cost.
3. "user_ad_history": Chronological log of individual user ad interactions.
4. "top_users_clicks": Top 10 most active users by click volume.
5. "top_advertisers_by_region": Top 5 highest-spending advertisers segmented by region.

## Performance Optimizations & Solved Challenges

Several classic Big Data bottlenecks were encountered and resolved during development:

1. Overcoming "OperationTimedOut" errors:
   * Challenge: Row-by-row insertion using "iterrows()" overwhelmed the local Cassandra node, causing network timeouts and slow ingestion.
   * Solution: Implemented batch insertion using "BatchStatement" and optimized iteration with "itertuples()", drastically reducing network round-trips.

2. NoSQL Data Modeling & "ALLOW FILTERING":
   * Challenge: Attempting to query data by non-partition key attributes triggered "unpredictable performance" errors and blocked queries.
   * Solution: Shifted the analytical heavy lifting to Python (pre-aggregation) before loading. For example, regional top 5s are calculated in Pandas so Cassandra only stores final results, allowing immediate SELECT queries without full table scans.

3. Data Hygiene:
   * Built strict attribute validation to prevent runtime "KeyError" exceptions during dataframe merges, ensuring pipeline stability across different data sources with inconsistent naming conventions.
