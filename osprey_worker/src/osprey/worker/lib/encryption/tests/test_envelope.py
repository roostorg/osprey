import pytest
from osprey.worker.lib.encryption.envelope import Envelope
from osprey.worker.lib.encryption.exception import EncryptionEnvelopeNotSetUpException

"""
Run this test after creating the test GCP resources required and only run this test locally.
The gcp_credential will use the default service account if empty string.
The credentials created from 'gcloud login auth' will not work.

Steps to test
1. Create KMS keyring resource
2. Create key inside key inside KMS keyring
3. Give service account role to encrypt/decrypt with keyring
4. Modify kek_uri to point to location of crypto key
5. Turn on test i.e. remove @pytest.mark.skip
6. Run unit tests
"""
kek_uri = 'gcp-kms://projects/project/locations/us-west1/keyRings/encryption-test/cryptoKeys/test-key'
gcp_credential_path = ''


@pytest.fixture(scope='function')
def envelope() -> Envelope:
    return Envelope(kek_uri=kek_uri, gcp_credential_path=gcp_credential_path)


@pytest.mark.skip(reason='this test should only be run manually')
@pytest.mark.parametrize('message', ('secret message text', 'another secret ///'))
def test_envelope_encryption_decryption(envelope: Envelope, message: str) -> None:
    message = 'this is a secret message, dont tell anyone'
    encrypted_message = envelope.encrypt(message)
    assert not encrypted_message == message
    decrypted_message = envelope.decrypt(encrypted_message)
    assert decrypted_message == message
    decrypted_message = envelope.decrypt(encrypted_message.encode())
    assert decrypted_message == message


@pytest.mark.skip(reason='this test should only be run manually')
@pytest.mark.parametrize('message', ('secret message text', 'another secret ///'))
def test_envelope_not_setup_exception(envelope: Envelope, message: str) -> None:
    message = 'this is a secret message, dont tell anyone'
    envelope = Envelope('', '')
    with pytest.raises(EncryptionEnvelopeNotSetUpException):
        envelope.encrypt(message)

    with pytest.raises(EncryptionEnvelopeNotSetUpException):
        envelope.decrypt(message)
