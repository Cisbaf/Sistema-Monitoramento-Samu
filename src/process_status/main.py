# KAFKA PROCESS STATUS
from controller import ProcessController
from src.utils.env import get_envs
from src.kafka_service.consumer import KafkaConsumerService

(
api_sso_url,
uri_conn_kafka,
topic_sso_data,
timeout_conn,
timeout_read 
) = get_envs(
    keys = [
        "URL_API_SSO",
        "KAFKA_URI",
        "TOPIC_SSO_DATA",
        "TIMEOUT_API_CONN",
        "TIMEOUT_API_READ"
    ]
)

controller = ProcessController(
    consumer=KafkaConsumerService(
        kafka_uri=uri_conn_kafka,
        topic=topic_sso_data, # consumer
        group_id="status_agent_consumer_group"
    ),
)


controller.run()