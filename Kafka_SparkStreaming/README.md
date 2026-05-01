# Real-Time Wikipedia Streaming Pipeline 🚀

A real-time data engineering pipeline that ingests, filters, and stores live Wikipedia edits. Built with **Apache Kafka**, **Apache Spark (Structured Streaming)**, and **Apache Cassandra**, all containerized via Docker.

## 🏗 Architecture & Tech Stack

*   **Data Source:** Server-Sent Events (SSE) from Wikipedia's RecentChanges stream.
*   **Message Broker:** Apache Kafka (3.7.0)
*   **Stream Processing:** Apache Spark 3.5.1 (PySpark Structured Streaming)
*   **Database:** Apache Cassandra 4.1
*   **Infrastructure:** Docker & Docker Compose

## 🗂 Pipeline Flow

1.  **Producer:** A Python app listens to the live Wikipedia stream and publishes raw JSON events to the Kafka `input` topic.
2.  **Filter Job (Spark):** Reads from the `input` topic, parses the nested JSON, filters out bot edits, and keeps only specific domains (e.g., `en.wikipedia.org`). Writes the cleaned data to the Kafka `processed` topic.
3.  **Cassandra Job (Spark):** Reads from the `processed` topic, ensures data types match (casting timestamps), and writes the final records to a Cassandra database.

## ⚙️ Setup & Installation

### 1. Start the Infrastructure
Launch Kafka, Cassandra, and the Spark cluster:
```bash
docker-compose up -d kafka-server cassandra-db spark-master spark-worker
```

### 2. Initialize Database and Topics
Create the required keyspace and table in Cassandra:
```bash
docker exec -i cassandra-db cqlsh < workspace/cassandra/init.cql
```

Create Kafka topics:
```bash
docker exec -it kafka-server /opt/kafka/bin/kafka-topics.sh --create --topic input --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it kafka-server /opt/kafka/bin/kafka-topics.sh --create --topic processed --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## 🚀 Running the Pipeline

### 1. Start the Data Producer:
```bash
docker-compose up -d producer-app
```

### 2. Submit Spark Streaming Jobs (Run in background):
Run the Filter Job:
```bash
docker exec -d spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/bitnami/spark/apps/filter_job.py
```

Run the Cassandra Sink Job:
```bash
docker exec -d spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
  /opt/bitnami/spark/apps/cassandra_job.py
```

## 📊 Verification

Check if data is flowing into the `processed` topic:
```bash
docker exec -it kafka-server /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic processed \
  --max-messages 5
```

Query the Cassandra database to see the final stored records:
```bash
docker exec -it cassandra-db cqlsh -e "SELECT * FROM wiki_space.processed_pages LIMIT 15;"
```