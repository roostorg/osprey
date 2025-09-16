import os

import grpc
from google.cloud.bigtable import Client

BIGTABLE_EMULATOR_HOST = 'BIGTABLE_EMULATOR_HOST'


def fix_bigtable_client_if_using_emulator(client: Client) -> None:
    """
    We call this if the bigtable client is being used in integration tests with a bigtable emulator.
    The emulator channel that is created doesn't actually work and we can't call it, so we
    have to replace it.
    https://github.com/GoogleCloudPlatform/cloud-sdk-docker/issues/253#issuecomment-972247899
    """

    def replace_emulator_channel_fn(transport, options):
        return grpc.insecure_channel(target=os.environ[BIGTABLE_EMULATOR_HOST])

    client._emulator_channel = replace_emulator_channel_fn  # type:ignore[method-assign]
