from dataclasses import dataclass
from src.kafka_service.consumer import KafkaConsumerService
import time

@dataclass
class ProcessController:
    consumer: KafkaConsumerService
    # producer: producer.CachedKafkaProducer

    def run(self):
        while True:
            data_consume = self.consumer.get_message()
            if not data_consume:
                return
            print("msg", data_consume)
            time.sleep(5)
        