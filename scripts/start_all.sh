#!/bin/bash
set -e

# Load environment variables (Finnhub key, S3 bucket, etc.)
set -a
source ~/.env
set +a

PROJECT_DIR=~/kafka-stock-market-portfolio
cd ~/kafka_2.13-3.8.0

echo "Starting Zookeeper..."
nohup env KAFKA_HEAP_OPTS="-Xmx256M -Xms256M" bin/zookeeper-server-start.sh config/zookeeper.properties > ~/zookeeper.log 2>&1 &

echo "Waiting 15s for Zookeeper..."
sleep 15

echo "Starting Kafka..."
nohup env KAFKA_HEAP_OPTS="-Xmx512M -Xms512M" bin/kafka-server-start.sh config/server.properties > ~/kafka.log 2>&1 &

echo "Waiting 30s for Kafka..."
sleep 30

echo "Creating Kafka topic (4 partitions)..."
python3 "$PROJECT_DIR/scripts/create_topic.py"

echo "Starting producers and consumers..."
nohup python3 "$PROJECT_DIR/producers/finnhub_producer.py" > ~/bitcoin_producer.log 2>&1 &
nohup python3 "$PROJECT_DIR/producers/demo_metals_producer.py" > ~/metals_producer.log 2>&1 &
nohup python3 "$PROJECT_DIR/consumers/s3_consumer_boto3.py" > ~/s3_consumer.log 2>&1 &

echo "Starting dashboard..."
nohup streamlit run "$PROJECT_DIR/dashboard/dashboard.py" --server.address 0.0.0.0 --server.port 8501 > ~/dashboard.log 2>&1 &

echo "All services started."
echo "Check logs: tail -f ~/kafka.log ~/bitcoin_producer.log ~/s3_consumer.log ~/dashboard.log"
