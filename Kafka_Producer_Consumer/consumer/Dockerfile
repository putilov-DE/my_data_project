FROM python:3.11-slim

WORKDIR /app

# Install Kafka library
RUN pip install --no-cache-dir confluent-kafka

# Copy the script
COPY consumer.py .

# Run the script in unbuffered mode
CMD ["python", "-u", "consumer.py"]