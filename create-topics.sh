#!/bin/bash

# Wait until Kafka is reachable
echo "Waiting for Kafka to become available..."
while ! /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list; do
  echo "Waiting for Kafka..."
  sleep 5
done

# Create topics (adjust settings as needed)
kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic meterReadingIngestionTopic --partitions 3 --replication-factor 1

kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic reviewIngestionDataTopic --partitions 3 --replication-factor 1

kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic dataBacklogDataTopic --partitions 3 --replication-factor 1

kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic dataAggregationTopic --partitions 3 --replication-factor 1

echo "Topics created (if they didn't exist already)."