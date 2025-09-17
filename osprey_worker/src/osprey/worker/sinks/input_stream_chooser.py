import random
from datetime import datetime, timedelta

from google.cloud import pubsub_v1
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from osprey.engine.executor.execution_context import Action
from osprey.worker.adaptor.plugin_manager import bootstrap_input_stream
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.sinks import InputStreamSource
from osprey.worker.sinks.sink.input_stream import (
    AsyncPubSubOspreyActionInputStream,
    BaseInputStream,
    StaticInputStream,
)
from osprey.worker.sinks.sink.osprey_coordinator_input_stream import OspreyCoordinatorInputStream
from osprey.worker.sinks.utils.acking_contexts import BaseAckingContext, NoopAckingContext
from osprey.worker.sinks.utils.kafka import PatchedKafkaConsumer


def get_rules_sink_input_stream(
    input_stream_source: InputStreamSource,
) -> BaseInputStream[BaseAckingContext[Action]]:
    """Based on the `input_stream_source` constructs a configured input stream that can be used to source events to
    classify. For more details, see `InputStreamSource`."""

    if input_stream_source == InputStreamSource.PUBSUB:
        config = CONFIG.instance()
        gcloud_project = config.get_str('PUBSUB_OSPREY_PROJECT_ID', 'osprey-dev')
        pubsub_subscription = config.get_str('PUBSUB_OSPREY_RULES_SINK_SUBSCRIPTION', 'rules-sink')
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(gcloud_project, pubsub_subscription)
        batch_size = config.get_int('PUBSUB_OSPREY_INPUT_BATCH_SIZE', 100)
        gevent_queue_size = config.get_int('PUBSUB_OSPREY_INPUT_STREAM_GEVENT_QUEUE_SIZE', 1000)
        kek_uri = config.get_str('PUBSUB_ENCRYPTION_KEY_URI', '')
        return AsyncPubSubOspreyActionInputStream(
            subscriber=subscriber,
            subscription_path=subscription_path,
            kek_uri=kek_uri,
            max_messages=batch_size,
            gevent_queue_size=gevent_queue_size,
        )

    elif input_stream_source == InputStreamSource.OSPREY_COORDINATOR:
        return OspreyCoordinatorInputStream(client_id='meow')

    elif input_stream_source == InputStreamSource.SYNTHETIC:
        random_actions = []
        action_types = [
            'dm_channel_created',
            'user_phone_verification_completed',
            'guild_joined',
            'guild_left',
            'report_submitted',
        ]
        usernames = ['person_a', 'person_b', 'person_c', 'person_d', 'person_e', 'person_f', 'person_g']
        http_headers_user_agent = [
            'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) osprey/0.0.306 Chrome/78.0.3',
            'Osprey/20414 CFNetwork/1126 Darwin/19.5.0',
        ]
        emails = [
            'watchanimeintheoffice@watchanimeintheoffice.com',
            'dorime@interimoayapare.com',
            'icarus@pippa.org',
            '/@test-entity-id-uri-encoding.com',
        ]
        ips = [
            '127.0.0.1',
            '231.11.46.123',
            '148.123.7.13',
            '191.201.12.201',
        ]

        now = datetime.utcnow()

        for i in range(random.randint(1000, 5000)):
            data = {
                'user': {
                    'id': random.getrandbits(32),
                    'username': random.choice(usernames),
                    'email': random.choice(emails),
                },
                'http_request': {
                    'headers': {'User-Agent': random.choice(http_headers_user_agent)},
                    'remote_addr': random.choice(ips),
                },
                'remote_addr_mobile': random.choice(ips),
                'guild': {'id': random.getrandbits(32), 'name': str(random.random())},
                'target': {'ip': random.choice(ips)},
            }

            random_actions.append(
                NoopAckingContext(
                    item=Action(action_id=i, action_name=random.choice(action_types), data=data, timestamp=now)
                )
            )
            now += timedelta(minutes=1)

        return StaticInputStream(random_actions)
    elif input_stream_source == InputStreamSource.KAFKA:
        config = CONFIG.instance()
        client_id = config.get_str('OSPREY_KAFKA_INPUT_STREAM_CLIENT_ID', 'localhost')
        input_topic: str = config.get_str('OSPREY_KAFKA_INPUT_STREAM_TOPIC', 'osprey.actions_input')
        input_bootstrap_servers: list[str] = config.get_str_list('OSPREY_KAFKA_BOOTSTRAP_SERVERS', ['localhost'])
        group_id = config.get_optional_str('OSPREY_KAFKA_GROUP_ID')

        consumer: PatchedKafkaConsumer = PatchedKafkaConsumer(
            input_topic,
            bootstrap_servers=input_bootstrap_servers,
            client_id=client_id,
            group_id=group_id,
            partition_assignment_strategy=(RoundRobinPartitionAssignor,),
        )
        from osprey.worker.sinks.sink.input_stream import KafkaInputStream

        return KafkaInputStream(
            kafka_consumer=consumer,
        )
    elif input_stream_source == InputStreamSource.PLUGIN:
        stream = bootstrap_input_stream()
        if stream is None:
            raise AssertionError('No input stream plugin registered')
        return stream
