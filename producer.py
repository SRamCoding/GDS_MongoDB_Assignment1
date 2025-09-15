# venv\Scripts\activate
# pip freeze > requirements.txt
import os
import time
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer


# ---------- Cargar variables de entorno ----------
load_dotenv()


# ---------- Callback de confirmación ----------
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed for record {msg.key()}: {err}")
    else:
        print(
            f"✅ Record {msg.key()} produced to {msg.topic()} "
            f"[{msg.partition()}] at offset {msg.offset()}"
        )


# ---------- Configuración Kafka + Schema Registry ----------
kafka_config = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP"),
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.getenv("KAFKA_API_KEY"),
    'sasl.password': os.getenv("KAFKA_API_SECRET"),
}

schema_registry_client = SchemaRegistryClient({
    'url': os.getenv("SCHEMA_REGISTRY_URL"),
    'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_KEY')}:{os.getenv('SCHEMA_REGISTRY_SECRET')}"
})

# Topic y Subject dinámicos
topic_name = os.getenv("KAFKA_TOPIC")
subject_name = os.getenv("SCHEMA_SUBJECT")

# Obtener Avro schema desde el Schema Registry
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Serializadores
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Producer
producer_conf = {
    **kafka_config,
    'key.serializer': key_serializer,
    'value.serializer': avro_serializer
}
producer = SerializingProducer(producer_conf)


# ---------- Leer CSV con Pandas y producir mensajes ----------
def produce_from_csv(file_path: str):
    df = pd.read_csv(file_path)
    
    df["TRANSPORTATION_DISTANCE_IN_KM"] = df["TRANSPORTATION_DISTANCE_IN_KM"].fillna(0).astype(int)
    
    df = df.replace({np.nan: None})
   
    df["Driver_MobileNo"] = (
                            df["Driver_MobileNo"]
                            .fillna("")                      # si hay NaN, los convierte en vacío
                            .astype(str)                     # convertir a string
                            .str.replace(r"\.0$", "", regex=True)  # eliminar .0 al final
                            )

    # Convertir a string el campo Minimum_kms_to_be_covered_in_a_day
    df["Minimum_kms_to_be_covered_in_a_day"] = (
        df["Minimum_kms_to_be_covered_in_a_day"]
        .fillna("")
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
    )


    for _, row in df.iterrows():
        try:
            record = row.to_dict()

            producer.produce(
                topic=topic_name,
                key=str(record.get("BookingID", None)),  # usaremos BookingID como key
                value=record,
                on_delivery=delivery_report
            )
            producer.poll(0)

        except Exception as e:
            print(f"❌ Error produciendo mensaje: {e}")

        # Delay opcional
        time.sleep(0.5)

    producer.flush()


if __name__ == "__main__":
    csv_file = "delivery_trip_truck_data.csv"
    produce_from_csv(csv_file)