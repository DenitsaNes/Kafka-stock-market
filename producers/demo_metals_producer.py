import json
import logging
import os
import random
import sys
import time
from kafka import KafkaProducer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from schema import MarketTradeEvent

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'demo_test')

KAFKA_RECONNECT_DELAY = 5


def create_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            logger.info(f"Retrying Kafka connection in {KAFKA_RECONNECT_DELAY}s...")
            time.sleep(KAFKA_RECONNECT_DELAY)


METALS = {
    'GC=F': 2500.00,
    'SI=F': 30.00,
    'PA=F': 1000.00,
    'PL=F': 950.00
}

DISPLAY_NAMES = {
    'GC=F': 'Gold (GC=F)',
    'SI=F': 'Silver (SI=F)',
    'PA=F': 'Palladium (PA=F)',
    'PL=F': 'Platinum (PL=F)'
}

current_prices = METALS.copy()

logger.info("Starting demo metals producer...")

producer = create_producer()

while True:
    for symbol, base_price in METALS.items():
        change_pct = random.uniform(-0.005, 0.005)
        current_prices[symbol] = current_prices[symbol] * (1 + change_pct)

        try:
            event = MarketTradeEvent(
                symbol=DISPLAY_NAMES[symbol],
                price=round(current_prices[symbol], 2),
                volume=round(random.uniform(0.1, 10.0), 2),
                timestamp_ms=int(time.time() * 1000),
                source='demo'
            )
        except Exception as e:
            logger.warning(f"Validation error (skipped): {symbol} — {e}")
            continue

        record = event.to_dict()
        key = record['symbol'].encode('utf-8')
        try:
            producer.send(KAFKA_TOPIC, key=key, value=record)
            logger.info(f"Sent demo: {record}")
        except Exception as e:
            logger.error(f"Failed to send to Kafka: {e}")
            producer = create_producer()

    try:
        producer.flush()
    except Exception as e:
        logger.error(f"Failed to flush Kafka producer: {e}")
        producer = create_producer()

    time.sleep(5)
