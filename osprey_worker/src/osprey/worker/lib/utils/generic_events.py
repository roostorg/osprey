import json
from typing import Any

from google.cloud import pubsub_v1

DEFAULT_PUBLISHER_CLIENT = pubsub_v1.PublisherClient()


def publish_generic_event(
    event: dict[str, Any],
    env: str,
    topic: str = 'projects/osprey-data-{}/topics/generic-events',
    client: pubsub_v1.PublisherClient | None = None,
) -> pubsub_v1.publisher.futures.Future:
    client = client or DEFAULT_PUBLISHER_CLIENT
    env_to_project_mapping = {'staging': 'stg', 'production': 'prd'}

    if env not in env_to_project_mapping:
        raise ValueError("`env` must be one of ('staging', 'production')")
    if 'event_type' not in event:
        raise ValueError('Event dict must contain key `event_type`')
    if ('timestamp' in event) and (not isinstance(event['timestamp'], int)):
        raise ValueError('event.timestamp must be formatted as long in unix epoch milliseconds')
    return client.publish(topic.format(env_to_project_mapping[env]), json.dumps(event).encode('utf-8'))
