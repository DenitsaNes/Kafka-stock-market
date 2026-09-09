import json
import logging
import os
import time
import websocket
from kafka import KafkaProducer

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from schema import MarketTradeEvent

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

API_KEY = os.getenv('FINNHUB_API_KEY')
if not API_KEY:
    raise ValueError("FINNHUB_API_KEY environment variable is not set")

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'demo_test')

FINNHUB_RECONNECT_DELAY = 5
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


producer = create_producer()


def on_open(ws):
    logger.info("Connected to Finnhub")
    ws.send(json.dumps({"type": "subscribe", "symbol": "BINANCE:BTCUSDT"}))


def on_message(ws, message):
    try:
        data = json.loads(message)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON from Finnhub: {e}")
        return

    logger.debug(f"Received: {data}")

    trades = data.get('data', []) if isinstance(data, dict) else []
    for trade in trades:
        try:
            event = MarketTradeEvent(
                symbol=trade.get('s', ''),
                price=float(trade.get('p', 0)),
                volume=float(trade.get('v', 0)),
                timestamp_ms=int(trade.get('t', 0)),
                source='finnhub'
            )
        except Exception as e:
            logger.warning(f"Validation error (skipped): {trade} — {e}")
            continue

        record = event.to_dict()
        key = record['symbol'].encode('utf-8')
        try:
            producer.send(KAFKA_TOPIC, key=key, value=record)
        except Exception as e:
            logger.error(f"Failed to send to Kafka: {e}")

    try:
        producer.flush()
    except Exception as e:
        logger.error(f"Failed to flush Kafka producer: {e}")


def on_error(ws, error):
    logger.error(f"Finnhub WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    logger.warning(f"Disconnected from Finnhub: {close_status_code} - {close_msg}")


def run():
    global producer
    while True:
        try:
            ws = websocket.WebSocketApp(
                f"wss://ws.finnhub.io?token={API_KEY}",
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever()
        except Exception as e:
            logger.error(f"Finnhub producer crashed: {e}")

        logger.info(f"Reconnecting to Finnhub in {FINNHUB_RECONNECT_DELAY}s...")
        time.sleep(FINNHUB_RECONNECT_DELAY)

        # Recreate the producer in case Kafka went down while we were running.
        producer = create_producer()


if __name__ == '__main__':
    run()
