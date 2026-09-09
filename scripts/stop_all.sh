#!/bin/bash

echo "Stopping all services..."

pkill -f zookeeper
pkill -f kafka-server-start
pkill -f finnhub_producer
pkill -f demo_metals_producer
pkill -f s3_consumer_boto3
pkill -f streamlit

echo "All services stopped."
