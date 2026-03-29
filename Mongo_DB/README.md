# AdTech Data Engineering Project: SQL vs MongoDB

This project demonstrates and compares ETL pipelines and analytical queries for an advertising platform using two different database paradigms: Relational (SQL) and Document-oriented (MongoDB).

## Project Structure

The repository is divided into two main environments:

### 1. `/sql_db` (Relational Approach)
- Contains normalized table schemas (Users, Campaigns, Events).
- Traditional SQL queries and JOINs for ad performance analysis.

### 2. `/mongo_db` (NoSQL / Document Approach)
- **Data Modeling**: Implements a "Matryoshka" (nested) schema where clicks are stored inside impressions, and impressions inside users.
- **ETL Process**: Python (Pandas & PyMongo) script transforming 2 million rows of raw CSV into nested JSON structures.
- **Advanced Analytics**: 5 complex MongoDB Aggregation Pipelines:
  - `task_1_user_history.json`: Full ad interaction history for a specific user.
  - `task_2_last_5_sessions.json`: Recent engagement tracking.
  - `task_3_ad_performance.json`: Hourly click analysis for advertisers.
  - `task_4_ad_fatigue.json`: Users with 5+ impressions and 0 clicks.
  - `task_5_top_categories.json`: Personalized ad targeting data based on top 3 categories.

## Tech Stack
- **Languages**: Python (Pandas, PyMongo)
- **Databases**: MongoDB (Docker), SQL
- **Formats**: JSON, BSON, CSV
