# Kafka Streaming Pipeline (Producer & Consumer)

## Project Description
This project is a streaming Data Engineering pipeline. It utilizes **Apache Kafka (in modern KRaft mode, without ZooKeeper)** for message routing and **Python** for data generation and consumption. 

The project is designed as a cohesive ecosystem with two main components running simultaneously:
1. **Kafka Producer:** Reads historical data from a CSV file (`tweets.csv`) and continuously sends it to a Kafka topic in JSON format at a rate of ~12 messages per second.
2. **Kafka Consumer:** Listens to the Kafka topic, parses the ISO-formatted timestamps (`created_at`) from the incoming JSON messages, and dynamically groups them. It saves the aggregated data every minute into new CSV files named by the exact minute the tweets were created.

## Tech Stack
* **Message Broker:** Apache Kafka (Official image `apache/kafka:3.7.0`)
* **Programming Language:** Python 3.11-slim
* **Containerization:** Docker & Docker Compose
* **Key Libraries:** `confluent-kafka`
* **Runtime Environment:** Tested and optimized for macOS (Apple Silicon M1)

## Project Structure
    .
    ├── .gitignore
    ├── README.md
    ├── docker-compose.yml           # Master infrastructure configuration
    ├── tweets.csv                   # Source data file (Required)
    ├── producer/                    # Data generation component
    │   ├── Dockerfile               # Producer Image
    │   └── producer.py              # Data sending script
    └── consumer/                    # Data processing component
        ├── Dockerfile               # Consumer Image
        ├── consumer.py              # Data aggregation script
        └── output_data/             # Generated CSV files destination

## Prerequisites
For local execution, ensure you have the following installed:
* Docker & Docker Compose (e.g., via OrbStack or Docker Desktop)
* Git

*Note: The source data file (`tweets.csv`) is not included in the repository due to its size. To run the pipeline, you must place a valid `tweets.csv` file in the **root directory** of the project.*

## Setup Instructions

### 1. Starting the Entire Pipeline
The project is configured as a monorepo. You can build and start the Kafka broker, the Producer, and the Consumer simultaneously with a single command. 

Navigate to the project root directory and run:
    docker-compose up --build -d

Ensure all three containers (`kafka-server`, `producer-app`, `consumer-app`) are successfully running:
    docker ps

### 2. Monitoring the Services
Both Python applications run in unbuffered mode, allowing you to monitor their activity in real-time.

To check the Producer (sending data):
    docker logs -f producer-app

To check the Consumer (receiving and saving data):
    docker logs -f consumer-app

### 3. Viewing the Results
Once the system has been running for a few minutes, navigate to the `consumer/output_data/` directory. You will see dynamically generated CSV files containing the extracted fields (`author_id`, `created_at`, `text`), named according to the data's internal timestamp:
`tweets_DD_MM_YYYY_HH_MM.csv`

### 4. Viewing Messages Directly in the Topic (Optional)
To verify that data is successfully arriving in Kafka independently of the Python consumer, run:
    docker exec -it kafka-server /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 \
      --topic tweets \
      --from-beginning

## Stopping the System
To gracefully stop the Producer, Consumer, and Kafka broker, and remove the virtual network, execute:
    docker-compose down

## Implementation Details
* **Single Node Kafka:** The Kafka broker is configured with `REPLICATION_FACTOR: 1` to ensure stable operation and topic creation in a single-container Docker environment.
* **ISO Time Parsing:** The consumer correctly parses complex ISO datetimes with microseconds (e.g., `2026-04-26T09:52:06.162358`) to accurately group messages into their corresponding minute-based files.