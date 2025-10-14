import streamlit as st
import os
import json
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import time

st.set_page_config(page_title="Kafka Test", layout="wide")

st.title("🔧 Tests de conectividad con Kafka")

# Obtener la configuración de Kafka desde las variables de entorno
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_REQUEST = os.environ.get("KAFKA_TOPIC", "predictions")
KAFKA_TOPIC_RESPONSE = os.environ.get("KAFKA_RESPONSE_TOPIC", "predictions-response")

st.subheader("Configuración")
st.write(f"**Bootstrap Servers:** `{KAFKA_BOOTSTRAP_SERVERS}`")
st.write(f"**Request Topic:** `{KAFKA_TOPIC_REQUEST}`")
st.write(f"**Response Topic:** `{KAFKA_TOPIC_RESPONSE}`")

# Test 1: Test de conexión
st.subheader("Test 1: Test de conexión")
if st.button("Testear la conexión a Kafka"):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=5000,
            retries=1
        )
        producer.close()
        st.success("Conexión exitosa")
    except Exception as e:
        st.error(f"Falló la conexión: {e}")

# Test 2: Enviar un mensaje
st.subheader("Test 2: Enviar un mensaje de texto")
if st.button("Enviar un mensaje de prueba"):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=5000
        )
        
        test_message = {
            "test": True,
            "timestamp": time.time(),
            "message": "This is a test message"
        }
        
        future = producer.send(KAFKA_TOPIC_REQUEST, value=test_message)
        record_metadata = future.get(timeout=10)
        producer.close()
        
        st.success(f"Mensaje enviado!")
        st.info(f"Topic: {record_metadata.topic}")
        st.info(f"Partition: {record_metadata.partition}")
        st.info(f"Offset: {record_metadata.offset}")
    except Exception as e:
        st.error(f"Falló el envío del mensaje: {e}")

# Test 3: Consumir un mensaje
st.subheader("Test 3: Consumir un mensaje")
if st.button("Consumir el último mensaje"):
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_REQUEST,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            consumer_timeout_ms=5000,
            group_id='streamlit_test_group'
        )
        
        messages = []
        for message in consumer:
            messages.append(message.value)
            if len(messages) >= 1:
                break
        
        consumer.close()
        
        if messages:
            st.success("Mensaje consumido exitosamente!")
            st.json(messages[0])
        else:
            st.warning("No hay mensajes (tópico vacío)")
    except Exception as e:
        st.error(f"Falló el consumo de mensajes: {e}")

# Test 4: Buscar los tópicos
st.subheader("Test 4: Verificar los tópicos")
if st.button("Consultar tópicos disponibles"):
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=5000
        )
        topics = consumer.topics()
        consumer.close()
        
        st.success("Conectado a Kafka")
        st.write("**Tópicos disponibles:**")
        for topic in topics:
            st.write(f"- `{topic}`")
    except Exception as e:
        st.error(f"Falló la verificación de los tópicos: {e}")