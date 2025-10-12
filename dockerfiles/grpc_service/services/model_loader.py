import json
import pickle
import boto3
import mlflow
import numpy as np

class ModelLoader:
    def __init__(self, model_name, alias):
        self.model = None
        self.version = None
        self.data_dict = None
        self.load_model(model_name, alias)

    def load_model(self, model_name, alias):
        try:
            mlflow.set_tracking_uri('http://mlflow:5000')
            client = mlflow.MlflowClient()
            model_data = client.get_model_version_by_alias(model_name, alias)
            self.model = mlflow.sklearn.load_model(model_data.source)
            self.version = int(model_data.version)
        except:
            with open('/app/files/model.pkl', 'rb') as f:
                self.model = pickle.load(f)
            self.version = 0

        try:
            s3 = boto3.client('s3')
            s3.head_object(Bucket='data', Key='data_info/data.json')
            result = s3.get_object(Bucket='data', Key='data_info/data.json')
            text = result["Body"].read().decode()
            self.data_dict = json.loads(text)
            self.data_dict["standard_scaler_mean"] = np.array(self.data_dict["standard_scaler_mean"])
            self.data_dict["standard_scaler_std"] = np.array(self.data_dict["standard_scaler_std"])
        except:
            with open('/app/files/data.json', 'r') as f:
                self.data_dict = json.load(f)

    def check_and_update(self, model_name, alias):
        try:
            mlflow.set_tracking_uri('http://mlflow:5000')
            client = mlflow.MlflowClient()
            new_model_data = client.get_model_version_by_alias(model_name, alias)
            new_version = int(new_model_data.version)
            if new_version != self.version:
                self.load_model(model_name, alias)
        except:
            pass