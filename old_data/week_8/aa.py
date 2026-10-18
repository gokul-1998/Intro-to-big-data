from kafka import KafkaProducer, KafkaConsumer
import time

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'hello-world'

# Producer
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: v.encode('utf-8'))

def send_message():
    producer.send(TOPIC, 'Hello World!')
    producer.flush()
    print("Message Sent")

send_message()

# Consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda v: v.decode('utf-8')
)

print("Waiting for messages...")
for message in consumer:
    print(f"Received: {message.value}")
