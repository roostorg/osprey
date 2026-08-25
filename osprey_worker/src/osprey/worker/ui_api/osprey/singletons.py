from osprey.worker.lib.publisher import BasePublisher, make_publisher
from osprey.worker.lib.singleton import Singleton
from osprey.worker.lib.singletons import CONFIG

from .lib.druid_client_holder import DruidClientHolder

DRUID: Singleton[DruidClientHolder] = Singleton(DruidClientHolder)


def _init_analytics_publisher() -> BasePublisher:
    config = CONFIG.instance()
    project = config.get_str('PUBSUB_DATA_PROJECT_ID', 'osprey-dev')
    topic = config.get_str('PUBSUB_ANALYTICS_EVENT_TOPIC_ID', 'osprey-analytics')
    return make_publisher(project, topic)


ANALYTICS_PUBLISHER: Singleton[BasePublisher] = Singleton(_init_analytics_publisher)
