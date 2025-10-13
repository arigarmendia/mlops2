import grpc
from concurrent import futures
import logging

# Import generated proto files
from proto import prediction_pb2
from proto import prediction_pb2_grpc

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PredictionServiceServicer(prediction_pb2_grpc.PredictionServiceServicer):
    """gRPC service for rain prediction - dummy implementation"""

    def Predict(self, request, context):
        """
        Single prediction endpoint - returns dummy response
        TODO: Implement actual model prediction logic similar to FastAPI service
        """
        logger.info(f"Received prediction request for location: {request.location}, date: {request.date}")

        # Dummy response - always predicts no rain
        return prediction_pb2.PredictionResponse(
            int_output=False,
            str_output="It won't rain tomorrow (dummy response)"
        )

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