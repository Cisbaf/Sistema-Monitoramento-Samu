import json
import time
import logging
from kafka import KafkaConsumer, errors
from dataclasses import dataclass, field
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

@dataclass
class KafkaConsumerService:
    kafka_uri: str
    topic: str
    group_id: str
    api_version: tuple = (3, 8)
    consumer: Optional[KafkaConsumer] = field(init=False, default=None)

    def __post_init__(self):
        self._connect()

    def _connect(self):
        """Conecta (ou reconecta) ao Kafka."""
        while True:
            try:
                self.consumer = KafkaConsumer(
                    self.topic,
                    api_version=self.api_version,
                    bootstrap_servers=self.kafka_uri,
                    auto_offset_reset="earliest",   # se não tiver offset salvo, começa do início
                    enable_auto_commit=True,        # offsets salvos automaticamente
                    group_id=self.group_id,
                    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                )
                logging.info("Conectado ao Kafka com sucesso.")
                break
            except errors.NoBrokersAvailable:
                logging.error("Nenhum broker disponível. Tentando novamente em 5s...")
                time.sleep(5)

    def get_message(self, timeout_ms: int = 1000):
        """
        Busca uma mensagem do tópico.
        Retorna None se não houver mensagem no timeout.
        """
        try:
            msg_pack = self.consumer.poll(timeout_ms=timeout_ms, max_records=1)
            for tp, messages in msg_pack.items():
                for message in messages:
                    return {
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "key": message.key.decode("utf-8") if message.key else None,
                        "value": message.value,
                    }
        except (errors.KafkaError, errors.KafkaTimeoutError) as e:
            logging.error(f"Erro no consumer: {e}. Tentando reconectar...")
            self._connect()
        return None

    def close(self):
        """Fecha o consumer corretamente."""
        if self.consumer:
            self.consumer.close()
            logging.info("Consumer fechado.")
