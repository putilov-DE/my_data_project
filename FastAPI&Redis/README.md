# AdTech Analytics API

A high-performance API for advertising campaign analytics, built with **FastAPI**, **MySQL**, and **Redis** for caching.

## ⚠️ Security Disclaimer
**Note:** In this repository, database credentials (logins and passwords) are hardcoded in the configuration files (`main.py`, `docker-compose.yml`). 
**This is done strictly for educational purposes** to make it easier to run and review the project locally. In a real production environment, credentials should be managed using `.env` files (ignored by Git) or a secure secrets manager.

## Project Architecture
The project follows a microservices approach, where each component runs in its own **isolated Docker container** within a shared Docker bridge network:

1. **API Container (FastAPI)**: The core backend logic. Handles incoming HTTP requests and orchestrates data flow between the cache and the database.
2. **Database Container (MySQL)**: The persistent storage layer. Completely isolated from the application code, storing relational data about ad campaigns, events, and users.
3. **Cache Container (Redis)**: An in-memory data store acting as a caching layer to reduce the load on the MySQL database and provide lightning-fast responses.

*Note: The containers communicate with each other internally using Docker's DNS resolution (e.g., the API connects to the database via the hostname `mysql` and to the cache via `redis`).*

## Endpoints
- `GET /campaign/{id}/performance`: Retrieves CTR, clicks, impressions, and total ad spend for a specific campaign.
- `GET /advertiser/{id}/spending`: Returns the total ad spend across all campaigns for a specific advertiser.
- `GET /user/{id}/engagements`: Returns a list of ad engagements (where a click/revenue occurred) for a specific user.

## Database Creation Commands (DDL)
To recreate the database structure for testing, run the following SQL commands in your MySQL instance:

```sql
CREATE DATABASE IF NOT EXISTS adtech;
USE adtech;

CREATE TABLE advertisers (
    advertiser_id INT AUTO_INCREMENT PRIMARY KEY,
    advertiser_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE campaigns (
    campaign_id INT PRIMARY KEY,
    advertiser_id INT,
    campaign_name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    target_age_min INT,
    target_age_max INT,
    target_interest VARCHAR(50),
    target_location_id INT,
    ad_slot_size VARCHAR(50),
    budget DECIMAL(12,2),
    remaining_budget FLOAT
);

CREATE TABLE events (
    event_id VARCHAR(36) PRIMARY KEY,
    campaign_id INT,
    user_id INT,
    device ENUM('Mobile','Desktop','Tablet'),
    location_id INT,
    event_timestamp DATETIME,
    bid_amount DECIMAL(12,2),
    ad_cost DECIMAL(12,2),
    was_clicked TINYINT(1),
    click_timestamp DATETIME,
    ad_revenue DECIMAL(12,2)
);
```

## Requirements
- Docker & Docker Compose
- Python 3.10+ (for running the local benchmark script)

## How to Run

1. **Clone the repository:**
   ```bash
   git clone https://github.com/putilov-DE/my_data_project.git
   cd my_data_project/<your_api_folder_name>
   ```

2. **Start the Docker containers:**
   ```bash
   docker-compose up -d --build
   ```

3. **Access the API Documentation:**
   Once running, the interactive Swagger UI is available at:
   [http://localhost:8000/docs](http://localhost:8000/docs)

## Performance Testing (Benchmarking)
A python script is included to measure the API response times with and without Redis caching.

1. Install the required library (locally):
   ```bash
   pip install requests
   ```
2. Run the benchmark:
   ```bash
   python benchmark.py
   ```

The script makes two consecutive requests to the endpoints:
- **First request (Cache Miss)**: Queries MySQL directly and caches the result.
- **Second request (Cache Hit)**: Retrieves data instantly from Redis.

Typically, the Redis cache speeds up the response time by 20x to 50x.