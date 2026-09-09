import json
import os
import random
import sys
import time
from kafka import KafkaProducer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from schema import MarketTradeEvent

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'demo_test')

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

METALS = {
    'GC=F': 2500.00,
    'SI=F': 30.00,
    'PA=F': 1000.00,
    'PL=F': 950.00
}

# Use shorter, display-friendly names in the symbol field for consistency.
DISPLAY_NAMES = {
    'GC=F': 'Gold (GC=F)',
    'SI=F': 'Silver (SI=F)',
    'PA=F': 'Palladium (PA=F)',
    'PL=F': 'Platinum (PL=F)'
}

current_prices = METALS.copy()

print("Starting demo metals producer...")

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
            print(f"Validation error (skipped): {symbol} — {e}")
            continue

        record = event.to_dict()
        key = record['symbol'].encode('utf-8')
        producer.send(KAFKA_TOPIC, key=key, value=record)
        print(f"Sent demo: {record}")

    producer.flush()
    time.sleep(5)
