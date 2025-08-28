from dataclasses import dataclass
from pydantic import BaseModel
from api_sso import queryApiData
from kafka_service.utils.producer import CachedKafkaProducer
import time
from typing import Optional
import xmltodict
from kafka_service.utils.agents import filter_agents


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
                dict_data = xmltodict.parse(request_sso.text)
                json_data = dict_data.get("system", {}).get("status_agente", [])
                json_filtered = filter_agents(json_data)
                try:
                    self.kafka.send(self.config.topic_sso_data, json_filtered)
                except Exception as e:
                    print("Erro no serviço do kafka >" + str(e))
            except Exception as e:
                print("Erro ao consultar api", str(e))
            time.sleep(self.config.sleep)
            
     