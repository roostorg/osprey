import logging
from dataclasses import dataclass

import tink
from tink import aead
from tink.integration import gcpkms


@dataclass
class EncryptionBase:
    kek_uri: str | None = None
    gcp_credential_path: str = ''
    is_setup: bool = False

    def __post_init__(self):
        try:
            aead.register()
        except tink.TinkError as e:
            logging.exception('Error initializing Tink: %s', e)
            return

        if self.kek_uri:
            # Read the GCP credentials and setup client
            try:
                gcpkms.GcpKmsClient.register_client(self.kek_uri, self.gcp_credential_path)
            except tink.TinkError as e:
                logging.exception('Error initializing GCP client: %s', e)
                return

        self.is_setup = True
