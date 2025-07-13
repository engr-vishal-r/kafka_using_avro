from confluent_kafka import Consumer
import avro.schema
import avro.io
import io
import os
from dotenv import load_dotenv

load_dotenv() 

# Load Avro schema
schema_path=os.getenv("AVRO_SCHEMA_PATH")
with open(schema_path, "r") as f:
    schema = avro.schema.parse(f.read())

# Kafka consumer config
conf = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    'group.id': 'test-consumer-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe([os.getenv("KAFKA_TOPIC")])

def deserialize_avro(msg_bytes, schema):
    bytes_reader = io.BytesIO(msg_bytes)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    return reader.read(decoder)

print("Waiting for messages...")
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue
    decoded = deserialize_avro(msg.value(), schema)
    print(f"Received message: {decoded}")