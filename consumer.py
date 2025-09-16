import os
import signal
from dotenv import load_dotenv
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from pymongo import MongoClient


# ---------- Cargar .env ----------
load_dotenv()


# ---------- Kafka Configuration ----------
kafka_config = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP"),
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.getenv("KAFKA_API_KEY"),
    'sasl.password': os.getenv("KAFKA_API_SECRET"),
    'group.id': os.getenv("KAFKA_GROUP_ID", "group1"),
    'auto.offset.reset': os.getenv("KAFKA_OFFSET_RESET", "earliest"),
}

# ---------- Schema Registry Client ----------
schema_registry_client = SchemaRegistryClient({
    'url': os.getenv("SCHEMA_REGISTRY_URL"),
    'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_KEY')}:{os.getenv('SCHEMA_REGISTRY_SECRET')}"
})

# Obtener el Avro schema
subject_name = os.getenv("SCHEMA_SUBJECT")
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# ---------- Deserializers ----------
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# ---------- Kafka Consumer ----------
consumer_conf = {
    **kafka_config,
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
}
consumer = DeserializingConsumer(consumer_conf)

# Suscribirse al topic
topic_name = os.getenv("KAFKA_TOPIC")
consumer.subscribe([topic_name])


# ---------- MongoDB ----------
mongo_uri = os.getenv("MONGO_URI")
mongo_db = os.getenv("MONGO_DB")
mongo_collection = os.getenv("MONGO_COLLECTION")

client = MongoClient(mongo_uri)
db = client[mongo_db]
collection = db[mongo_collection]


# ---------- Graceful Shutdown ----------
running = True
def shutdown(sig, frame):
    global running
    running = False
    print("\n🛑 Shutdown signal received, closing consumer...")
signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


# ---------- Consuming Loop ----------
print(f"📡 Listening for messages on topic '{topic_name}'...")

while running:
    try:
        msg = consumer.poll(1.0)  # espera 1 segundo
        if msg is None:
            continue

        if msg.error():
            print(f"⚠️ Consumer error: {msg.error()}")
            continue

        record_key = msg.key()
        record_value = msg.value()  # dict ya deserializado por Avro
        print(f"✅ Received record key={record_key}, value={record_value}")

        # Insertar en MongoDB
        if record_value:
            collection.insert_one(record_value)

    except Exception as e:
        print(f"❌ Error in consumer loop: {e}")
        continue

# Cierre limpio
consumer.close()
client.close()
print("👋 Consumer stopped and MongoDB connection closed.")
