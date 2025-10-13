import os
import json
import pickle
import grpc
import boto3
import mlflow
import numpy as np
import pandas as pd

from datetime import datetime

from concurrent import futures
import logging

# Import generated proto files
import prediction_pb2
import prediction_pb2_grpc

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# Utilidades
# =========================

def load_model(model_name: str, alias: str):
    try:
        mlflow.set_tracking_uri('http://mlflow:5000')
        client_mlflow = mlflow.MlflowClient()
        model_data_mlflow = client_mlflow.get_model_version_by_alias(model_name, alias)
        model_ml = mlflow.sklearn.load_model(model_data_mlflow.source)
        version_model_ml = int(model_data_mlflow.version)
    except:
        file_ml = open('/app/files/model.pkl', 'rb')
        model_ml = pickle.load(file_ml)
        file_ml.close()
        version_model_ml = 0

    try:
        s3 = boto3.client('s3')
        s3.head_object(Bucket='data', Key='data_info/data.json')
        result_s3 = s3.get_object(Bucket='data', Key='data_info/data.json')
        text_s3 = result_s3["Body"].read().decode()
        data_dictionary = json.loads(text_s3)
        data_dictionary["standard_scaler_mean"] = np.array(data_dictionary["standard_scaler_mean"])
        data_dictionary["standard_scaler_std"] = np.array(data_dictionary["standard_scaler_std"])
    except:
        file_s3 = open('/app/files/data.json', 'r')
        data_dictionary = json.load(file_s3)
        file_s3.close()

    return model_ml, version_model_ml, data_dictionary


def get_season(dt: pd.Timestamp) -> str:
    m = dt.month
    if m in [12, 1, 2]:
        return 'Summer'
    if m in [3, 4, 5]:
        return 'Fall'
    if m in [6, 7, 8]:
        return 'Winter'
    return 'Spring'


def transform_features_raw_to_model_input(req, data_dict):
    """
    Reproduce la transformación de tu FastAPI:
    - parsea fecha → Year/Month/Day/Season → sin/cos
    - mapea direcciones de viento a grados + sin/cos
    - location → lat/long
    - reordena columnas
    - cast dtypes
    - escala con StandardScaler (mean/std del data_dict)
    """
    # 1) construir DataFrame con los nombres "originales" de FastAPI
    # notá que en protobuf usamos snake_case; acá mapeamos a los nombres originales
    raw = {
        "Date": req.date,
        "Location": req.location,
        "MinTemp": req.min_temp,
        "MaxTemp": req.max_temp,
        "Rainfall": req.rainfall,
        "Evaporation": req.evaporation,
        "Sunshine": req.sunshine,
        "WindGustDir": req.wind_gust_dir,
        "WindGustSpeed": req.wind_gust_speed,
        "WindDir9am": req.wind_dir_9am,
        "WindDir3pm": req.wind_dir_3pm,
        "WindSpeed9am": req.wind_speed_9am,
        "WindSpeed3pm": req.wind_speed_3pm,
        "Humidity9am": req.humidity_9am,
        "Humidity3pm": req.humidity_3pm,
        "Pressure9am": req.pressure_9am,
        "Pressure3pm": req.pressure_3pm,
        "Cloud9am": req.cloud_9am,
        "Cloud3pm": req.cloud_3pm,
        "Temp9am": req.temp_9am,
        "Temp3pm": req.temp_3pm,
        "RainToday": req.rain_today,
    }
    df = pd.DataFrame([raw])

    # 2) Fecha → Year/Month/Day/Season + encoding
    df['Date'] = pd.to_datetime(df['Date'])
    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month
    df['Day'] = df['Date'].dt.day

    df['Season'] = df['Date'].apply(get_season)
    df.drop(columns='Date', inplace=True)

    df['SeasonDegree'] = df['Season'].map(data_dict['season_degrees'])
    df['Season_sin'] = np.sin(np.deg2rad(df['SeasonDegree']))
    df['Season_cos'] = np.cos(np.deg2rad(df['SeasonDegree']))
    df.drop(columns=['Season', 'SeasonDegree'], inplace=True)

    # 3) Direcciones de viento → grados + sin/cos (y dropear las originales)
    for dir_var in data_dict['wind_dir_columns']:
        df[dir_var] = df[dir_var].map(data_dict['wind_dir_degrees'])
        df[f'{dir_var}_sin'] = np.sin(np.deg2rad(df[dir_var]))
        df[f'{dir_var}_cos'] = np.cos(np.deg2rad(df[dir_var]))
        df.drop(columns=[dir_var], inplace=True)

    # 4) Location → (lat, long)
    df[['Latitude', 'Longitude']] = df['Location'].apply(
        lambda x: pd.Series(data_dict['city_coordinates'][x])
    )
    df.drop(columns=['Location'], inplace=True)

    # 5) Reordenar columnas y castear dtypes
    df = df[data_dict["columns_after_transform"]]
    df = df.astype(data_dict["columns_dtypes_after_transform"])

    # 6) Estandarizar (StandardScaler)
    df = (df - data_dict["standard_scaler_mean"]) / data_dict["standard_scaler_std"]
    return df


# =========================
# Carga en el arranque
# =========================

model, version_model, data_dict = load_model("rain_in_australia_model_prod", "champion")

# =========================
# Implementación gRPC
# =========================

class PredictionServiceServicer(prediction_pb2_grpc.PredictionServiceServicer):
    """gRPC service for rain prediction - dummy implementation"""

    def Predict(self, request, context):
        """
        Single prediction endpoint - returns dummy response
        """
        try:
            features_df = transform_features_raw_to_model_input(request, data_dict)
            pred = model.predict(features_df)  # 0/1
            int_output = bool(int(pred[0]))
            str_output = "It won't rain tomorrow" if not int_output else "It will rain tomorrow"
            return prediction_pb2.PredictionResponse(int_output=int_output, str_output=str_output)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return prediction_pb2.PredictionResponse(int_output=False, str_output="error")

    def PredictStream(self, request_iterator, context):
        """
        Streaming prediction endpoint - returns dummy responses
        TODO: Implement batch prediction logic
        """
        for request in request_iterator:
            logger.info(f"Received streaming prediction request for location: {request.location}")

            # Dummy response for each request
            yield prediction_pb2.PredictionResponse(
                int_output=False,
                str_output="It won't rain tomorrow (dummy response)"
            )


def serve():
    """Start the gRPC server"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    prediction_pb2_grpc.add_PredictionServiceServicer_to_server(
        PredictionServiceServicer(), server
    )

    server.add_insecure_port('[::]:50051')
    logger.info("Starting gRPC server on port 50051...")
    server.start()
    logger.info("gRPC server started successfully")

    server.wait_for_termination()


if __name__ == '__main__':
    serve()