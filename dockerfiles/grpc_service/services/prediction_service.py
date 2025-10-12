import numpy as np
import pandas as pd
from concurrent import futures
import grpc
from generated import prediction_pb2, prediction_pb2_grpc
from services.model_loader import ModelLoader
from config.settings import settings

class PredictionServicer(prediction_pb2_grpc.PredictionServiceServicer):
    def __init__(self, model_loader):
        self.model_loader = model_loader

    def Predict(self, request, context):
        try:
            features_df = self._preprocess(request)
            prediction = self.model_loader.model.predict(features_df)
            str_pred = "It will rain tomorrow" if prediction[0] > 0 else "It won't rain tomorrow"
            return prediction_pb2.PredictionResponse(
                int_output=bool(prediction[0].item()),
                str_output=str_pred
            )
        except Exception as e:
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return prediction_pb2.PredictionResponse()

    def PredictStream(self, request_iterator, context):
        """Handle streaming predictions"""
        self.model_loader.check_and_update(settings.MODEL_NAME, settings.MODEL_ALIAS)
        for request in request_iterator:
            try:
                features_df = self._preprocess(request)
                prediction = self.model_loader.model.predict(features_df)
                str_pred = "It will rain tomorrow" if prediction[0] > 0 else "It won't rain tomorrow"
                yield prediction_pb2.PredictionResponse(
                    int_output=bool(prediction[0].item()),
                    str_output=str_pred
                )
            except Exception as e:
                context.set_details(str(e))
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

    def _preprocess(self, request):
        """Convert protobuf request to processed DataFrame (same logic as FastAPI)"""
        data = {
            'Date': request.date,
            'Location': request.location,
            'MinTemp': request.min_temp,
            'MaxTemp': request.max_temp,
            'Rainfall': request.rainfall,
            'Evaporation': request.evaporation,
            'Sunshine': request.sunshine,
            'WindGustDir': request.wind_gust_dir,
            'WindGustSpeed': request.wind_gust_speed,
            'WindDir9am': request.wind_dir_9am,
            'WindDir3pm': request.wind_dir_3pm,
            'WindSpeed9am': request.wind_speed_9am,
            'WindSpeed3pm': request.wind_speed_3pm,
            'Humidity9am': request.humidity_9am,
            'Humidity3pm': request.humidity_3pm,
            'Pressure9am': request.pressure_9am,
            'Pressure3pm': request.pressure_3pm,
            'Cloud9am': request.cloud_9am,
            'Cloud3pm': request.cloud_3pm,
            'Temp9am': request.temp_9am,
            'Temp3pm': request.temp_3pm,
            'RainToday': request.rain_today,
        }
        features_df = pd.DataFrame([data])
        
        # Apply same transformations as FastAPI
        features_df['Date'] = pd.to_datetime(features_df['Date'])
        features_df['Year'] = features_df['Date'].dt.year
        features_df['Month'] = features_df['Date'].dt.month
        features_df['Day'] = features_df['Date'].dt.day
        features_df['Season'] = features_df['Date'].apply(self._get_season)
        features_df.drop(columns='Date', inplace=True)

        features_df['SeasonDegree'] = features_df['Season'].map(self.model_loader.data_dict['season_degrees'])
        features_df['Season_sin'] = np.sin(np.deg2rad(features_df['SeasonDegree']))
        features_df['Season_cos'] = np.cos(np.deg2rad(features_df['SeasonDegree']))
        features_df.drop(columns=["Season", "SeasonDegree"], inplace=True)

        for dir_var in self.model_loader.data_dict['wind_dir_columns']:
            features_df[dir_var] = features_df[dir_var].map(self.model_loader.data_dict['wind_dir_degrees'])
            features_df[f'{dir_var}_sin'] = np.sin(np.deg2rad(features_df[dir_var]))
            features_df[f'{dir_var}_cos'] = np.cos(np.deg2rad(features_df[dir_var]))
            features_df.drop(columns=[dir_var], inplace=True)

        features_df[['Latitude', 'Longitude']] = features_df['Location'].apply(
            lambda x: pd.Series(self.model_loader.data_dict['city_coordinates'][x])
        )
        features_df.drop(columns=['Location'], inplace=True)

        features_df = features_df[self.model_loader.data_dict["columns_after_transform"]]
        features_df = features_df.astype(self.model_loader.data_dict["columns_dtypes_after_transform"])
        features_df = (features_df - self.model_loader.data_dict["standard_scaler_mean"]) / self.model_loader.data_dict["standard_scaler_std"]

        return features_df

    @staticmethod
    def _get_season(date):
        month = date.month
        if month in [12, 1, 2]:
            return 'Summer'
        elif month in [3, 4, 5]:
            return 'Fall'
        elif month in [6, 7, 8]:
            return 'Winter'
        elif month in [9, 10, 11]:
            return 'Spring'