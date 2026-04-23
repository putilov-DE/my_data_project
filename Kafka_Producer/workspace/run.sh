#!/bin/bash
docker run -d \
  --name producer-app \
  --network kafka-network \
  kafka-producer:v1