from dataclasses import dataclass
from pydantic import BaseModel
from api_sso import queryApiData
from src.kafka_service.producer import CachedKafkaProducer
import time
from typing import Optional
import xmltodict
from src.utils.agents import filter_agents, remove_fields_not_utilized, normalize_datetime_fields
from src.utils.date import current_time_sp

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
                now = current_time_sp()
                request_sso = queryApiData(
                    url=self.config.api_sso_url,
                    timeout_conn=int(self.config.timeout_conn),
                    timeout_read=int(self.config.timeout_read)
                )
                dict_data = xmltodict.parse(request_sso.text)
                json_filtered = filter_agents(dict_data, hours=1)
                json_clean = remove_fields_not_utilized(json_filtered)
                json_normalized = normalize_datetime_fields(json_clean, now)
                try:
                    self.kafka.send(self.config.topic_sso_data, json_normalized)
                except Exception as e:
                    print("Erro no serviço do kafka >" + str(e))
            except Exception as e:
                print("Erro ao consultar api", str(e))
            time.sleep(self.config.sleep)
            
     