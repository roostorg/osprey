from osprey.worker.lib.publisher import BasePublisher, NullPublisher, PubSubPublisher
from osprey.worker.lib.singleton import Singleton
from osprey.worker.lib.singletons import CONFIG

from .lib.clickhouse_client_holder import ClickHouseClientHolder
from .lib.druid_client_holder import DruidClientHolder

DRUID: Singleton[DruidClientHolder] = Singleton(DruidClientHolder)
CLICKHOUSE: Singleton[ClickHouseClientHolder] = Singleton(ClickHouseClientHolder)


def _init_analytics_publisher() -> BasePublisher:
    config = CONFIG.instance()
    if config.get_bool('OSPREY_DISABLE_ANALYTICS_PUBLISHER', False):
        return NullPublisher()
    project = config.get_str('PUBSUB_DATA_PROJECT_ID', 'osprey-dev')
    topic = config.get_str('PUBSUB_ANALYTICS_EVENT_TOPIC_ID', 'osprey-analytics')
    return PubSubPublisher(project, topic)


ANALYTICS_PUBLISHER: Singleton[BasePublisher] = Singleton(_init_analytics_publisher)
