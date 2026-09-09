#!/bin/bash

echo "Stopping all services..."

# Stop Streamlit dashboard
pkill -9 -f streamlit

# Stop Python producers and consumer
pkill -f finnhub_producer
pkill -f demo_metals_producer
pkill -f s3_consumer_boto3

# Stop Kafka gracefully if the stop script exists
if [ -x ~/kafka_2.13-3.8.0/bin/kafka-server-stop.sh ]; then
    ~/kafka_2.13-3.8.0/bin/kafka-server-stop.sh
    sleep 10
fi
# Force-kill any remaining Kafka processes
pkill -9 -f kafka-server-start

# Stop Zookeeper gracefully if the stop script exists
if [ -x ~/kafka_2.13-3.8.0/bin/zookeeper-server-stop.sh ]; then
    ~/kafka_2.13-3.8.0/bin/zookeeper-server-stop.sh
    sleep 5
fi
# Force-kill any remaining Zookeeper processes
pkill -9 -f zookeeper

echo "All services stopped."
