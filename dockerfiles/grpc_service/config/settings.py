import os

class Settings:
    MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    MODEL_NAME = "rain_in_australia_model_prod"
    MODEL_ALIAS = "champion"
    S3_BUCKET = "data"
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "predictions")
    GRPC_PORT = int(os.getenv("GRPC_PORT", 50051))

settings = Settings()