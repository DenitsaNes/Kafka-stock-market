import os
import sys
from kafka.admin import KafkaAdminClient, NewTopic

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'demo_test')

admin = KafkaAdminClient(bootstrap_servers=[KAFKA_BROKER], client_id='topic-creator')

topic = NewTopic(
    name=KAFKA_TOPIC,
    num_partitions=4,
    replication_factor=1
)

existing = admin.list_topics()
if KAFKA_TOPIC in existing:
    print(f"Topic '{KAFKA_TOPIC}' already exists. Delete it first if you want to change partitions.")
    sys.exit(0)

admin.create_topics([topic])
print(f"Created topic '{KAFKA_TOPIC}' with 4 partitions and replication factor 1")
admin.close()
