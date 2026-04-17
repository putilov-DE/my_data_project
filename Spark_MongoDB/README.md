# Amazon Reviews ETL Pipeline (PySpark & MongoDB)

This project implements a classic ETL pipeline for big data processing. The pipeline reads Amazon review data, performs aggregations using PySpark, and saves the resulting data marts into a MongoDB NoSQL database.

## 🛠 Tech Stack
* **Python 3.12**
* **Apache Spark 3.5.1** (PySpark)
* **MongoDB 7.0**
* **Docker & Docker Compose** (infrastructure deployment)
* **MongoDB Connector for Spark 10.4.0**

## 🏗 Project Architecture
The infrastructure is deployed using Docker containers:
1. **Spark Master & Worker**: Compute cluster.
2. **MongoDB**: Storage for the processed data (data marts).

## 🚀 How to Run the Project

### 1. Environment Setup
Ensure you have Docker (or OrbStack) and Java 17 installed on your machine.
```bash
git clone <your_repository_link>
cd amazon_reviews_etl
```

### 2. Start the Infrastructure
Deploy the Spark cluster and MongoDB with a single command:
```bash
docker-compose up -d
```

### 3. Install Dependencies (Local setup)
```bash
python -m venv venv
source venv/bin/activate
pip install pyspark==3.5.1
```

### 4. Execute the ETL Pipeline
Run the script via Jupyter Notebook or PyCharm. The script will perform the following steps:
1. Initialize a Spark session with the MongoDB connector.
2. Read the raw data from `amazon_reviews.csv`.
3. Calculate three distinct data marts:
   - Product statistics (`product_stats`).
   - Customer activity (`customer_activity`).
   - Monthly product trends (`product_trends`).
4. Write the aggregated results to MongoDB.

## 📊 Results
After successful execution, the processed data can be explored using MongoDB Compass at `mongodb://localhost:27017/`. 
The `amazon_db` database will contain three collections with ready-made aggregations for further analytics.