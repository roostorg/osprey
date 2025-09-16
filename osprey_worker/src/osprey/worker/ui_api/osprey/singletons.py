from osprey.worker.lib.publisher import PubSubPublisher
from osprey.worker.lib.singleton import Singleton
from osprey.worker.lib.singletons import CONFIG, ENGINE
from osprey.worker.sinks.sink.output_sink import EventEffectsOutputSink

from .lib.druid_client_holder import DruidClientHolder

DRUID: Singleton[DruidClientHolder] = Singleton(DruidClientHolder)

config = CONFIG.instance()
EVENT_EFFECT_SINK: Singleton[EventEffectsOutputSink] = Singleton(
    lambda: EventEffectsOutputSink(
        engine=ENGINE.instance(),
        analytics_publisher=PubSubPublisher(
            config.get_str('PUBSUB_DATA_PROJECT_ID', 'osprey-dev'),
            config.get_str('PUBSUB_ANALYTICS_EVENT_TOPIC_ID', 'osprey-analytics'),
        ),
        webhooks_publisher=PubSubPublisher(
            config.get_str('PUBSUB_OSPREY_WEBHOOKS_PROJECT_ID', 'osprey-dev'),
            config.get_str('PUBSUB_OSPREY_WEBHOOKS_TOPIC_ID', 'osprey-webhooks'),
        ),
    )
)


def _init_analytics_publisher() -> PubSubPublisher:
    config = CONFIG.instance()
    project = config.get_str('PUBSUB_DATA_PROJECT_ID', 'osprey-dev')
    topic = config.get_str('PUBSUB_ANALYTICS_EVENT_TOPIC_ID', 'osprey-analytics')
    return PubSubPublisher(project, topic)


ANALYTICS_PUBLISHER: Singleton[PubSubPublisher] = Singleton(_init_analytics_publisher)
