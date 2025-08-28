import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from collections import deque
import threading
import time
from datetime import datetime


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()  # Exemplo: "2025-03-20T09:23:14"
        return super().default(o)
    
class CachedKafkaProducer:
    def __init__(self, kafka_uri, api_version=(3, 8, 0)):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_uri,
            api_version=api_version,
            value_serializer=lambda v: json.dumps(v, cls=DateTimeEncoder).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if isinstance(k, str) else k,
            max_block_ms=1000,
            retries=0,
        )
        self.queue = deque()
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self._process_queue, daemon=True)
        self.running = True
        self.thread.start()

    def send(self, topic, value, key=None):
        """Adiciona a mensagem à fila para envio ordenado."""
        with self.lock:
            self.queue.append((topic, value, key))

    def _process_queue(self):
        """Processa a fila em ordem, tentando enviar cada mensagem com timeout de 1s."""
        while self.running:
            if not self.queue:
                time.sleep(0.1)
                continue

            # Obtém a primeira mensagem sem remover da fila
            with self.lock:
                if not self.queue:
                    continue
                topic, value, key = self.queue[0]

            try:
                print(topic)
                future = self.producer.send(topic, value=value, key=key)
                print(future)
                future.get(timeout=3)  # Aumente conforme necessário
                # Remove da fila após sucesso
                with self.lock:
                    self.queue.popleft()
            except (KafkaError, TimeoutError) as e:
                print(f"Erro ao enviar ({topic}-{key}): {e}. Retentando...")
                time.sleep(1)  # Espera antes de retentar
            except Exception as e:
                print(f"Erro inesperado: {e}")
                time.sleep(1)

    def close(self):
        """Encerra o produtor e a thread de background."""
        self.running = False
        self.thread.join()
        self.producer.close()