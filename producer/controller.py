from dataclasses import dataclass
from pydantic import BaseModel
from api_sso import queryApiData
from kafka_service import CachedKafkaProducer
import time
from typing import Optional

class ProducerConfig(BaseModel):
    api_sso_url: str
    timeout_conn: str
    timeout_read: str
    topic_sso_data: str
    sleep: Optional[int] = 1

@dataclass
class ProducerController:
    config: ProducerConfig
    kafka: CachedKafkaProducer

    def run(self):
        while True:
            try:
                request_sso = queryApiData(
                    url=self.config.api_sso_url,
                    timeout_conn=int(self.config.timeout_conn),
                    timeout_read=int(self.config.timeout_read)
                )
                print(request_sso)
                try:
                    self.kafka.send(self.config.topic_sso_data, request_sso.text)
                except Exception as e:
                    print("Erro no serviço do kafka >" + str(e))
            except Exception as e:
                print("Erro ao consultar api", str(e))

            time.sleep(self.config.sleep)
            
     