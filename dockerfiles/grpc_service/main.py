import grpc
from concurrent import futures
import prediction_pb2
import prediction_pb2_grpc

# Dummy para probar gRPC
class PredictionServiceServicer(prediction_pb2_grpc.PredictionServiceServicer):
    def Predict(self, request, context):
        return prediction_pb2.PredictionResponse()

    def PredictStream(self, request_iterator, context):
        yield prediction_pb2.PredictionResponse()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    prediction_pb2_grpc.add_PredictionServiceServicer_to_server(
        PredictionServiceServicer(), server
    )
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()