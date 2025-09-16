# import time
# from concurrent import futures
# from unittest.mock import MagicMock

import pytest
from google.cloud import pubsub_v1
from osprey.worker.lib.pubsub.publisher_client import BatchPubsubPublisherClient


@pytest.fixture(scope='function')
def batch_pubsub_publisher() -> BatchPubsubPublisherClient:
    batch_settings = pubsub_v1.types.BatchSettings(
        max_bytes=2000000,  # default 1MB
        max_messages=20,  # default 100 messages
        max_latency=1,
    )
    return BatchPubsubPublisherClient(batch_settings=batch_settings)


# def test_batch_publishing(pubsub_client_mock, batch_pubsub_publisher) -> None:
#     num_messages = 50
#
#     def mock_publish():
#         time.sleep(0.05)
#
#     with futures.ThreadPoolExecutor(max_workers=5) as executor:
#         pubsub_client_mock.publish = MagicMock(return_value=executor.submit(mock_publish))
#         for i in range(num_messages):
#             batch_pubsub_publisher.publish('test', b'data')
#
#     assert pubsub_client_mock.publish.call_count == 50
#     batch_pubsub_publisher.stop()
#     assert pubsub_client_mock.stop.call_count == 1
#     assert batch_pubsub_publisher._futures_buffer == set()
