from osprey.worker.lib.publisher import KafkaPublisher, PubSubPublisher
from osprey.worker.lib.singleton import Singleton
from osprey.worker.lib.singletons import CONFIG

from .lib.druid_client_holder import DruidClientHolder

DRUID: Singleton[DruidClientHolder] = Singleton(DruidClientHolder)


def _init_analytics_publisher() -> PubSubPublisher:
    config = CONFIG.instance()
    project = config.get_str('PUBSUB_DATA_PROJECT_ID', 'osprey-dev')
    topic = config.get_str('PUBSUB_ANALYTICS_EVENT_TOPIC_ID', 'osprey-analytics')
    return PubSubPublisher(project, topic)


def _init_kafka_analytics_publisher() -> KafkaPublisher:
    config = CONFIG.instance()
    bootstrap_servers = config.get_str_list('OSPREY_KAFKA_BOOTSTRAP_SERVERS', ['localhost'])
    client_id = config.get_str('OSPREY_KAFKA_INPUT_STREAM_CLIENT_ID', 'osprey-dev')
    topic = config.get_str('OSPREY_KAFKA_ANALYTICS_TOPIC', 'osprey-analytics')
    return KafkaPublisher(bootstrap_servers, client_id, topic)


def get_analytics_publisher() -> Singleton[PubSubPublisher] | Singleton[KafkaPublisher]:
    with open('/sys/class/dmi/id/product_name', 'r') as pn:
        if 'Google' in pn:
            ANALYTICS_PUBLISHER: Singleton[PubSubPublisher] = Singleton(_init_analytics_publisher)
        else:
            ANALYTICS_PUBLISHER: Singleton[KafkaPublisher] = Singleton(_init_kafka_analytics_publisher)  # type: ignore[no-redef]

    return ANALYTICS_PUBLISHER
