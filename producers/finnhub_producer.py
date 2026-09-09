import json
import os
import websocket
from kafka import KafkaProducer

API_KEY = os.getenv('FINNHUB_API_KEY')
if not API_KEY:
    raise ValueError("FINNHUB_API_KEY environment variable is not set")

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'demo_test')

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)


def on_open(ws):
    print("Connected to Finnhub")
    ws.send(json.dumps({"type": "subscribe", "symbol": "BINANCE:BTCUSDT"}))


def on_message(ws, message):
    data = json.loads(message)
    print(f"Received: {data}")

    # Finnhub returns a batch of trades under the 'data' key.
    # Use the trade symbol as the Kafka key so all trades for the
    # same symbol land in the same partition.
    if isinstance(data, dict) and 'data' in data and isinstance(data['data'], list):
        for trade in data['data']:
            key = trade.get('s', 'unknown').encode('utf-8')
            producer.send(KAFKA_TOPIC, key=key, value=trade)
    else:
        key = data.get('s', 'unknown').encode('utf-8')
        producer.send(KAFKA_TOPIC, key=key, value=data)

    producer.flush()


def on_error(ws, error):
    print(f"Error: {error}")


def on_close(ws, close_status_code, close_msg):
    print("Disconnected from Finnhub")


ws = websocket.WebSocketApp(
    f"wss://ws.finnhub.io?token={API_KEY}",
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

ws.run_forever()
