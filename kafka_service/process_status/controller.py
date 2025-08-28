from dataclasses import dataclass
from kafka_service.utils import consumer
import time


@dataclass
class ProcessController:
    consumer: consumer.KafkaConsumerService
    # producer: producer.CachedKafkaProducer

    def run(self):
        while True:
            data_consume = self.consumer.get_message()
            if not data_consume:
                return
            print("msg", data_consume)
            time.sleep(5)
        