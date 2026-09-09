import json
import logging
import os
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
import boto3

from consumers.s3_upload import upload_to_s3

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
if not BUCKET_NAME:
    raise ValueError("S3_BUCKET_NAME environment variable is not set")

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'market.trades.raw')
DLQ_TOPIC = f"{KAFKA_TOPIC}.dlq"

BASE_DELAY = 1


def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='s3-sink-consumer-group'
    )


def create_dlq_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )


def send_to_dlq(producer, original_message, error_reason):
    try:
        payload = {
            'original_message': original_message,
            'error': error_reason,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        producer.send(DLQ_TOPIC, value=payload)
        producer.flush()
        logger.warning(f"Sent message to DLQ: {error_reason}")
    except Exception as e:
        logger.error(f"Failed to send message to DLQ: {e}")


def run():
    s3 = boto3.client('s3')
    dlq_producer = create_dlq_producer()

    while True:
        try:
            consumer = create_consumer()
            logger.info("S3 consumer started. Waiting for messages...")

            for count, message in enumerate(consumer):
                try:
                    timestamp = datetime.now(timezone.utc).strftime('%Y/%m/%d/%H%M%S_%f')
                    filename = f"raw/{timestamp}_{count}.json"

                    upload_to_s3(s3, BUCKET_NAME, filename, json.dumps(message.value))
                    logger.info(f"Saved: s3://{BUCKET_NAME}/{filename}")
                except Exception as e:
                    logger.error(f"Failed to process message: {e}")
                    send_to_dlq(dlq_producer, message.value, str(e))

        except Exception as e:
            logger.error(f"Consumer crashed: {e}")
            logger.info(f"Reconnecting in {BASE_DELAY}s...")
            time.sleep(BASE_DELAY)


if __name__ == '__main__':
    run()
