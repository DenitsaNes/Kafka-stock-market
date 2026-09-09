import os
import sys
from kafka.admin import KafkaAdminClient, NewTopic

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'market.trades.raw')
DLQ_TOPIC = f"{KAFKA_TOPIC}.dlq"

def create_topic(admin, name, partitions, replication_factor=1):
    existing = admin.list_topics()
    if name in existing:
        print(f"Topic '{name}' already exists. Delete it first if you want to change partitions.")
        return False

    topic = NewTopic(
        name=name,
        num_partitions=partitions,
        replication_factor=replication_factor
    )
    admin.create_topics([topic])
    print(f"Created topic '{name}' with {partitions} partitions and replication factor {replication_factor}")
    return True


try:
    admin = KafkaAdminClient(bootstrap_servers=[KAFKA_BROKER], client_id='topic-creator')
    create_topic(admin, KAFKA_TOPIC, 4)
    create_topic(admin, DLQ_TOPIC, 1)
finally:
    admin.close()
