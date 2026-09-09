import json
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer
import boto3

BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
if not BUCKET_NAME:
    raise ValueError("S3_BUCKET_NAME environment variable is not set")

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'demo_test')

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

s3 = boto3.client('s3')

print("S3 consumer started. Waiting for messages...")

for count, message in enumerate(consumer):
    timestamp = datetime.now(timezone.utc).strftime('%Y/%m/%d/%H%M%S_%f')
    filename = f"raw/{timestamp}_{count}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=filename,
        Body=json.dumps(message.value)
    )

    print(f"Saved: s3://{BUCKET_NAME}/{filename}")
