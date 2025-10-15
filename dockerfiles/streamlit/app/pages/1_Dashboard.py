import streamlit as st
import pandas as pd
import awswrangler as wr
import boto3
import matplotlib.pyplot as plt
import os
import json
import time
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime

# Config de Kafka
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_REQUEST = os.environ.get("KAFKA_TOPIC", "predictions")
KAFKA_TOPIC_RESPONSE = os.environ.get("KAFKA_RESPONSE_TOPIC", "predictions-response")

# Configurar la sesión de boto3 para conectarse a MinIO
session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
)

# Configurar el cliente S3 para MinIO
s3_client = session.client(
    service_name='s3',
    endpoint_url='http://s3:9000'
)

# Leer el archivo CSV desde MinIO usando awswrangler
bucket_name = "data"
file_path = "raw/weatherAUS.csv"

try:
    data = wr.s3.read_csv(path=f's3://{bucket_name}/{file_path}', boto3_session=session)
except Exception as e:
    st.error(f"Error al conectar con MinIO o al leer el archivo: {e}")


def send_prediction_request(request_data):
    """Pedir la predicción al tópico de Kafka"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        
        # Agrego un timestamp con ID para poder correlacionar el pedido con la respuesta
        request_data['timestamp'] = datetime.now().isoformat()
        request_data['request_id'] = int(time.time() * 1000)  # Agrego otra componente al ID para que en lo posible sea único
        
        producer.send(KAFKA_TOPIC_REQUEST, value=request_data)
        producer.flush()
        producer.close()
        
        return request_data['request_id']
    except Exception as e:
        st.error(f"Error enviando el pedido a Kafka: {e}")
        return None


def consume_prediction_response(request_id, timeout=30):
    """Consumir la respuesta (predicción) desde el tópico de Kafka"""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_RESPONSE,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            consumer_timeout_ms=timeout * 1000,
            group_id='streamlit_responses',
            enable_auto_commit=True
        )
        start_time = time.time()

        for message in consumer:
            response = message.value
            # Checkear si la respuesta coincide con el pedido
            if response.get('request_id') == request_id:
                consumer.close()
                return response
            # Timeout if taking too long
            if time.time() - start_time > timeout:
                consumer.close()
                return None

        consumer.close()
        return None
    
    except Exception as e:
        st.error(f"Error consumiendo la respuesta desde Kafka: {e}")
        return None


st.write("# Trabajo práctico final de la materia Aprendizaje de Máquina 2")
st.write("##### *Esta página fue realizada con Streamlit y en ella podrá interactuar con el modelo de ML para predecir lluvias basado en el dataset Rain in Australia*")

st.image("rain_aust.png", caption="")

tab1, tab2, tab3, tab4 = st.tabs(["Datasets", "Gráficos", "Predicción", "Encuesta"])


#-------------------------------- PESTAÑA 1 - DATASET -------------------------------
with tab1:

    st.header("Descripción del dataset")
    st.write("Datos:", data.describe())
    st.divider()

    st.header("Dataset - Primeros 50 datos")
    st.write("Datos:", data.head(50))

    @st.cache_data
    def convert_df(df):
        return df.to_csv(index=False).encode("utf-8")

    csv = convert_df(data)

    st.download_button(
        label="Descargar dataset completo como CSV",
        data=csv,
        file_name="Dataset_RIA.csv",
        mime="text/csv",
    )
    st.divider()

    st.header("Tipos de datos del dataset")
    st.write("Datos:", data.dtypes)
    st.divider()


#-------------------------------- PESTAÑA 2 - GRAFICOS -------------------------------
with tab2:

    st.header("Vista de Gráficos")

    with st.container():
        if 'data' in locals() and data is not None:
            data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
            data['Month'] = data['Date'].dt.to_period('M').astype(str)
            observations_per_month = data['Month'].value_counts().sort_index()

            st.title('Rain in Australia')
            st.write('### Cantidad de Observaciones por Mes')
            st.bar_chart(observations_per_month)

            st.write("Datos cargados correctamente y visualización generada.")
        else:
            st.warning("No se pudieron cargar los datos. Verifica la conexión y la ruta del archivo.")


#-------------------------------- PESTAÑA 3 - PREDICCION -------------------------------
with tab3:

    st.header("Ingrese los datos y luego oprima el botón Predecir")

    # fecha = st.date_input("Fecha a predecir", value=None)
    fecha = st.date_input("Fecha a predecir", value=datetime.now().date())

    unique_locations = data['Location'].unique()
    Location = st.selectbox('Seleccione una ubicación', unique_locations)

    MinTemp = st.number_input("MinTemp", min_value=None, max_value=None, value=13.6)
    MaxTemp = st.number_input("MaxTemp", min_value=None, max_value=None, value=28.3)
    Rainfall = st.number_input("Rainfall", min_value=None, max_value=None, value=2.6)
    Evaporation = st.number_input("Evaporation", min_value=None, max_value=None, value=3.5)
    Sunshine = st.number_input("Temperatura mínima", min_value=None, max_value=None, value=5.2)

    dir = ["E", "ENE", "ESE", "N", "NE", "NNE", "NNW", "NW", "S", "SE", "SSE", "SSW", "SW", "W", "WNW", "WSW"]
    WindGustDir = st.selectbox('WindGustDir', dir)

    WindGustSpeed = st.number_input("WindGustSpeed", min_value=None, max_value=None, value=44.3)

    WindDir9am = st.selectbox('WindDir9am', dir)
    WindDir3pm = st.selectbox('WindDir3pm', dir)

    WindSpeed9am = st.number_input("WindSpeed9am", min_value=None, max_value=None, value=41.9)
    WindSpeed3pm = st.number_input("WindSpeed3pm", min_value=None, max_value=None, value=43.5)
    Humidity9am = st.number_input("Humidity9am", min_value=None, max_value=None, value=68.6)
    Humidity3pm = st.number_input("Humidity3pm", min_value=None, max_value=None, value=81.3)
    Pressure9am = st.number_input("Pressure9am", min_value=None, max_value=None, value=1008)
    Pressure3pm = st.number_input("Pressure3pm", min_value=None, max_value=None, value=1007.6)
    Cloud9am = st.number_input("Cloud9am", min_value=None, max_value=None, value=6)
    Cloud3pm = st.number_input("Cloud3pm", min_value=None, max_value=None, value=8)
    Temp9am = st.number_input("Temp9am", min_value=None, max_value=None, value=16.7)
    Temp3pm = st.number_input("Temp3pm", min_value=None, max_value=None, value=25.6)

    RainToday = st.checkbox("RainToday")

    if st.button("Predecir"):
        processing = st.empty()
        processing.write("Enviando petición a Kafka...")

        request_data = {
            "date": str(fecha),
            "location": Location,
            "min_temp": float(MinTemp),
            "max_temp": float(MaxTemp),
            "rainfall": float(Rainfall),
            "evaporation": float(Evaporation),
            "sunshine": float(Sunshine),
            "wind_gust_dir": WindGustDir,
            "wind_gust_speed": float(WindGustSpeed),
            "wind_dir_9am": WindDir9am,
            "wind_dir_3pm": WindDir3pm,
            "wind_speed_9am": float(WindSpeed9am),
            "wind_speed_3pm": float(WindSpeed3pm),
            "humidity_9am": float(Humidity9am),
            "humidity_3pm": float(Humidity3pm),
            "pressure_9am": float(Pressure9am),
            "pressure_3pm": float(Pressure3pm),
            "cloud_9am": int(Cloud9am),
            "cloud_3pm": int(Cloud3pm),
            "temp_9am": float(Temp9am),
            "temp_3pm": float(Temp3pm),
            "rain_today": bool(RainToday)
        }

        # Send request to Kafka
        request_id = send_prediction_request(request_data)

        if request_id:
            processing.write("Esperando respuesta del modelo...")
            
            # Wait for response
            response = consume_prediction_response(request_id, timeout=30)

            processing.empty()

            if response:
                int_output = response.get('int_output', False)
                str_output = response.get('str_output', 'Sin respuesta')

                if int_output:
                    st.image("lluvia.png", caption="")
                    st.write("### Anda buscando el paraguas maestro!")
                else:
                    st.image("sol.png", caption="")
                    st.write("### Dale tranquilo con el baile al aire libre, va estar más seco que lengua de loro")

                st.write("Respuesta completa:", response)
            else:
                st.error("No se recibió respuesta del modelo en el tiempo esperado. Intenta nuevamente.")
        else:
            processing.empty()
            st.error("No se pudo enviar la petición a Kafka.")


#-------------------------------- PESTAÑA 4 - ENCUESTA -------------------------------
with tab4:

    with st.form("Encuesta"):
        st.write("##### *Emita su opinión sobre el TP final de materia*")
        checkbox_val1 = st.checkbox("Fantástico")
        checkbox_val2 = st.checkbox("Inmejorable")
        checkbox_val3 = st.checkbox("Tienen un 10+ felicitado")

        submitted = st.form_submit_button("Submit")
        if submitted:
            st.write("Gracias por participar")
