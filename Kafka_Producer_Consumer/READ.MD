# Kafka Streaming Pipeline (Producer & Consumer)

## Project Description
This project is a streaming Data Engineering pipeline. It utilizes **Apache Kafka (in modern KRaft mode, without ZooKeeper)** for message routing and **Python** for data generation and consumption. 

The project is divided into two independent parts:
1. **Kafka_Producer:** Reads historical data from a CSV file (`tweets.csv`) and continuously sends it to a Kafka topic in JSON format at a rate of ~12 messages per second.
2. **Kafka_Consumer:** Listens to the Kafka topic and aggregates the received messages every minute, saving them into new CSV files on the local disk.

## Tech Stack
* **Message Broker:** Apache Kafka (Bitnami image 3.7.0)
* **Programming Language:** Python 3.11-slim
* **Containerization:** Docker & Docker Compose
* **Key Libraries:** `confluent-kafka`
* **Runtime Environment:** Tested and optimized for macOS (Apple Silicon M1)

## Project Structure
    .
    ├── .gitignore
    ├── README.md
    ├── Kafka_Producer/
    │   ├── docker-compose.yml       # Infrastructure (Kafka Server)
    │   ├── Dockerfile               # Producer Image
    │   └── workspace/
    │       ├── producer.py          # Data sending script
    │       ├── build.sh             # Container build script
    │       └── run.sh               # Producer run script
    └── Kafka_Consumer/              # (Part 2)
        ├── Dockerfile
        └── workspace/
            ├── consumer.py
            └── output_data/

## Prerequisites
For local execution, ensure you have the following installed:
* Docker & Docker Compose (e.g., via OrbStack or Docker Desktop)
* Git

*Note: The source data file (`tweets.csv`) is not included in the repository due to its size. To run the Producer, you must place a valid CSV file in the root of the `Kafka_Producer` directory.*

## Setup Instructions (Part 1: Producer)

### 1. Starting the Kafka Infrastructure
Navigate to the Producer folder and start Kafka:
    cd Kafka_Producer
    docker-compose up -d

Ensure the `kafka-server` container is successfully running (using the `docker ps` command).

### 2. Building and Running the Producer
Build the Docker image for the Python script:
    bash workspace/build.sh

Start sending data:
    bash workspace/run.sh

To check the sending logs, use:
    docker logs -f producer-app

### 3. Viewing Messages in the Topic
To verify that data is successfully arriving in Kafka, run the following command to read the topic from inside the container:
    docker exec -it kafka-server /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 \
      --topic tweets \
      --from-beginning

## Stopping the System
To properly stop the Producer and remove the Kafka virtual network, execute:
    docker stop producer-app
    docker rm producer-app
    docker-compose down