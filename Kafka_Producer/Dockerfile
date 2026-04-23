FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir confluent-kafka
COPY tweets.csv .
COPY workspace/ ./workspace/
CMD ["python", "-u", "workspace/producer.py"]