import logging
from base64 import b64decode, b64encode

import tink
from osprey.worker.lib.encryption.base import EncryptionBase
from osprey.worker.lib.encryption.exception import EncryptionEnvelopeNotSetUpException
from tink import JsonKeysetReader, aead, cleartext_keyset_handle
from tink.proto import tink_pb2


class Envelope(EncryptionBase):
    associated_data: bytes = b''
    dek_template: tink_pb2.KeyTemplate = aead.aead_key_templates.AES256_GCM
    keyset_b64: str | None = None  # Optional for local env based aead encryption

    def __post_init__(self):
        super().__post_init__()
        try:
            if self.kek_uri:
                template = aead.aead_key_templates.create_kms_envelope_aead_key_template(
                    kek_uri=self.kek_uri,
                    dek_template=self.dek_template,
                )
                handle = tink.KeysetHandle.generate_new(template)
                self._env_aead = handle.primitive(aead.Aead)
            elif self.keyset_b64:
                keyset_json = b64decode(self.keyset_b64).decode()
                handle = cleartext_keyset_handle.read(JsonKeysetReader(keyset_json))
                self._env_aead = handle.primitive(aead.Aead)
            else:
                logging.error('Neither kek_uri nor keyset_b64 provided; cannot initialize encryption envelope')
                self.is_setup = False
        except Exception as e:
            self.is_setup = False
            logging.exception('Error initializing Encryption Envelope client: %s', e)

    def encrypt(self, plaintext: str | bytes, base64: bool = True) -> bytes:
        """
        plaintext: the message to encrypt
        base64: base64 encode the encrypted output (useful for json encoding)
        """
        if self.is_setup is not True:
            raise EncryptionEnvelopeNotSetUpException()

        if isinstance(plaintext, str):
            plaintext = plaintext.encode()

        encrypted: bytes = self._env_aead.encrypt(plaintext, self.associated_data)

        if base64:
            encrypted = b64encode(encrypted)

        return encrypted

    def decrypt(self, ciphertext: bytes, base64: bool = True) -> bytes:
        """
        ciphertext: bytes to decrypt
        base64: true if the ciphertext is base64 encoded (and must be b64decoded before decryption)
        decode: true if the plaintext was a string and should be decoded
        """
        if self.is_setup is not True:
            raise EncryptionEnvelopeNotSetUpException()

        if base64:
            ciphertext = b64decode(ciphertext)

        decrypted: bytes = self._env_aead.decrypt(ciphertext, self.associated_data)

        return decrypted
