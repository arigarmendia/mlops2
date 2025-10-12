import logging
from concurrent import futures
import grpc
from services.prediction_service import PredictionServicer
from services.model_loader import ModelLoader
from config.settings import settings
from kafka_consumer.kafka_handler import KafkaHandler
from generated import prediction_pb2_grpc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def serve():
    # Initialize model loader
    model_loader = ModelLoader(settings.MODEL_NAME, settings.MODEL_ALIAS)
    
    # Create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    prediction_pb2_grpc.add_PredictionServiceServicer_to_server(
        PredictionServicer(model_loader), server
    )
    server.add_insecure_port(f'[::]:{settings.GRPC_PORT}')
    
    logger.info(f"Starting gRPC server on port {settings.GRPC_PORT}")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()