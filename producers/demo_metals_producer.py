import json
import os
import random
import time
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'demo_test')

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

METALS = {
    'Gold (GC=F)': 2500.00,
    'Silver (SI=F)': 30.00,
    'Palladium (PA=F)': 1000.00,
    'Platinum (PL=F)': 950.00
}

current_prices = METALS.copy()

print("Starting demo metals producer...")

while True:
    for symbol, base_price in METALS.items():
        change_pct = random.uniform(-0.005, 0.005)
        current_prices[symbol] = current_prices[symbol] * (1 + change_pct)

        data = {
            's': symbol,
            'p': round(current_prices[symbol], 2),
            't': int(time.time() * 1000),
            'v': round(random.uniform(0.1, 10.0), 2)
        }

        key = symbol.encode('utf-8')
        producer.send(KAFKA_TOPIC, key=key, value=data)
        print(f"Sent demo: {data}")

    producer.flush()
    time.sleep(5)
