# KAFKA PRODUCER
from kafka_service.utils.env import get_envs
from controller import ProducerController, ProducerConfig, CachedKafkaProducer

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

producer = ProducerController(
    config = ProducerConfig(
        api_sso_url=api_sso_url,
        timeout_conn=timeout_conn,
        timeout_read=timeout_read,
        topic_sso_data=topic_sso_data,
    ),
    kafka=CachedKafkaProducer(
        kafka_uri=uri_conn_kafka
    )
)

producer.run()