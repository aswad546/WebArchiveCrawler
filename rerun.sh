#!/bin/bash

# Start Zookeeper and Kafka services
docker-compose up -d zookeeper kafka

# Wait for 15 seconds to ensure services are up and running
echo "Waiting for Kafka and Zookeeper to initialize..."
sleep 15

# Create Kafka topic with 10 partitions and a replication factor of 1
docker-compose exec kafka kafka-topics.sh --create --topic web_archive_script_extraction --partitions 10 --replication-factor 1 --bootstrap-server kafka:9092

# Start 10 consumer instances
docker-compose up -d --scale consumer=10

echo "Kafka topic created and consumers scaled up."
