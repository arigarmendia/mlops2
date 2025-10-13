import json
import pickle
import boto3
import mlflow
import datetime
import numpy as np
import pandas as pd

import strawberry
from strawberry.fastapi import GraphQLRouter
from fastapi import FastAPI

# ======================================================
# Funciones auxiliares
# ======================================================

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


model, version_model, data_dict = load_model("rain_in_australia_model_prod", "champion")

# ======================================================
# Definición del esquema GraphQL
# ======================================================

@strawberry.type
class ModelOutput:
    int_output: bool
    str_output: str


@strawberry.type
class Query:
    @strawberry.field
    def hello(self) -> str:
        return "Welcome to the Rain in Australia GraphQL API"

    @strawberry.field
    def predict(
        self,
        Date: str,
        Location: str,
        MinTemp: float,
        MaxTemp: float,
        Rainfall: float,
        Evaporation: float,
        Sunshine: float,
        WindGustDir: str,
        WindGustSpeed: float,
        WindDir9am: str,
        WindDir3pm: str,
        WindSpeed9am: float,
        WindSpeed3pm: float,
        Humidity9am: float,
        Humidity3pm: float,
        Pressure9am: float,
        Pressure3pm: float,
        Cloud9am: int,
        Cloud3pm: int,
        Temp9am: float,
        Temp3pm: float,
        RainToday: bool
    ) -> ModelOutput:
        features = {
            "Date": Date, "Location": Location, "MinTemp": MinTemp, "MaxTemp": MaxTemp,
            "Rainfall": Rainfall, "Evaporation": Evaporation, "Sunshine": Sunshine,
            "WindGustDir": WindGustDir, "WindGustSpeed": WindGustSpeed,
            "WindDir9am": WindDir9am, "WindDir3pm": WindDir3pm,
            "WindSpeed9am": WindSpeed9am, "WindSpeed3pm": WindSpeed3pm,
            "Humidity9am": Humidity9am, "Humidity3pm": Humidity3pm,
            "Pressure9am": Pressure9am, "Pressure3pm": Pressure3pm,
            "Cloud9am": Cloud9am, "Cloud3pm": Cloud3pm,
            "Temp9am": Temp9am, "Temp3pm": Temp3pm, "RainToday": RainToday
        }

        features_df = pd.DataFrame([features])
        features_df["Date"] = pd.to_datetime(features_df["Date"])
        features_df["Year"] = features_df["Date"].dt.year
        features_df["Month"] = features_df["Date"].dt.month
        features_df["Day"] = features_df["Date"].dt.day

        month_to_season = {
            12: 'Summer', 1: 'Summer', 2: 'Summer',
            3: 'Fall', 4: 'Fall', 5: 'Fall',
            6: 'Winter', 7: 'Winter', 8: 'Winter',
            9: 'Spring', 10: 'Spring', 11: 'Spring'
        }

        features_df["Season"] = features_df["Month"].map(month_to_season)
        features_df.drop(columns="Date", inplace=True)
        features_df["SeasonDegree"] = features_df["Season"].map(data_dict["season_degrees"])
        features_df["Season_sin"] = np.sin(np.deg2rad(features_df["SeasonDegree"]))
        features_df["Season_cos"] = np.cos(np.deg2rad(features_df["SeasonDegree"]))
        features_df.drop(columns=["Season", "SeasonDegree"], inplace=True)

        for dir_var in data_dict["wind_dir_columns"]:
            features_df[dir_var] = features_df[dir_var].map(data_dict["wind_dir_degrees"])
            features_df[f"{dir_var}_sin"] = np.sin(np.deg2rad(features_df[dir_var]))
            features_df[f"{dir_var}_cos"] = np.cos(np.deg2rad(features_df[dir_var]))
            features_df.drop(columns=[dir_var], inplace=True)

        features_df[["Latitude", "Longitude"]] = features_df["Location"].apply(
            lambda x: pd.Series(data_dict["city_coordinates"][x])
        )
        features_df.drop(columns=["Location"], inplace=True)
        features_df = features_df[data_dict["columns_after_transform"]]
        features_df = features_df.astype(data_dict["columns_dtypes_after_transform"])
        features_df = (features_df - data_dict["standard_scaler_mean"]) / data_dict["standard_scaler_std"]

        prediction = model.predict(features_df)
        str_pred = "It won't rain tomorrow" if prediction[0] == 0 else "It will rain tomorrow"
        return ModelOutput(int_output=bool(prediction[0]), str_output=str_pred)


schema = strawberry.Schema(query=Query)
graphql_app = GraphQLRouter(schema)

app = FastAPI()
app.include_router(graphql_app, prefix="/graphql")


@app.get("/")
def root():
    return {"message": "GraphQL server for Rain in Australia is running"}
