from kafka import KafkaConsumer
import json
import logging
from generated import prediction_pb2

logger = logging.getLogger(__name__)

class KafkaHandler:
    def __init__(self, bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer = None

    def connect(self):
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='grpc_prediction_group'
            )
            logger.info(f"Connected to Kafka topic: {self.topic}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def consume_messages(self):
        """Yield messages from Kafka"""
        if not self.consumer:
            self.connect()
        
        for message in self.consumer:
            try:
                data = message.value
                yield self._dict_to_protobuf(data)
            except Exception as e:
                logger.error(f"Error processing message: {e}")

    @staticmethod
    def _dict_to_protobuf(data):
        """Convert dict to PredictionRequest protobuf"""
        return prediction_pb2.PredictionRequest(
            date=data.get('date', ''),
            location=data.get('location', ''),
            min_temp=data.get('min_temp', 0),
            max_temp=data.get('max_temp', 0),
            rainfall=data.get('rainfall', 0),
            evaporation=data.get('evaporation', 0),
            sunshine=data.get('sunshine', 0),
            wind_gust_dir=data.get('wind_gust_dir', ''),
            wind_gust_speed=data.get('wind_gust_speed', 0),
            wind_dir_9am=data.get('wind_dir_9am', ''),
            wind_dir_3pm=data.get('wind_dir_3pm', ''),
            wind_speed_9am=data.get('wind_speed_9am', 0),
            wind_speed_3pm=data.get('wind_speed_3pm', 0),
            humidity_9am=data.get('humidity_9am', 0),
            humidity_3pm=data.get('humidity_3pm', 0),
            pressure_9am=data.get('pressure_9am', 0),
            pressure_3pm=data.get('pressure_3pm', 0),
            cloud_9am=data.get('cloud_9am', 0),
            cloud_3pm=data.get('cloud_3pm', 0),
            temp_9am=data.get('temp_9am', 0),
            temp_3pm=data.get('temp_3pm', 0),
            rain_today=data.get('rain_today', False),
        )