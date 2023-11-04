#!/bin/bash

# Kafka broker address
BROKER_ADDRESS="localhost:9092"

# Kafka topics to create
TOPICS=(
  "json-enrichment"
  "json-enrichment-output"
  "suspicious-user-activity-output"
)

# Number of partitions (default)
PARTITIONS=4

# Replication factor (default)
REPLICATION_FACTOR=1

# Function to check if Kafka is ready
wait_for_kafka() {
  echo "Waiting for Kafka to be ready..."
  while ! kafka-topics --list --bootstrap-server $BROKER_ADDRESS > /dev/null 2>&1; do
    echo "Kafka not ready, waiting 5 seconds..."
    sleep 5
  done
  echo "Kafka is ready."
}

wait_for_kafka

# Create Kafka topics
for TOPIC in "${TOPICS[@]}"
do
  kafka-topics --create --if-not-exists --bootstrap-server $BROKER_ADDRESS --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $TOPIC
done

echo "Topics creation script executed."
