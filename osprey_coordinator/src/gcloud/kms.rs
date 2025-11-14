use anyhow::{anyhow, Result};
use base64::engine::{general_purpose, Engine as _};
use prost::Message;
use tink_aead::subtle::AesGcm;
use tink_core::Aead;
use tonic::codegen::InterceptedService;

use crate::gcloud::auth::AuthorizationHeaderInterceptor;
use crate::gcloud::google::crypto::tink::AesGcmKey;
use crate::gcloud::google::kms::v1 as proto;
use crate::gcloud::google::kms::v1::key_management_service_client::KeyManagementServiceClient;
use crate::gcloud::grpc::connection::Connection;

pub const GOOGLE_KMS_DOMAIN: &str = "cloudkms.googleapis.com";

const GCP_PREFIX: &str = "gcp-kms://";

impl Connection {
    pub fn create_kms_aes_gcm_envelope(
        &self,
        key_uri: String,
        associated_data: Vec<u8>,
        base64_encoded: bool,
    ) -> Result<AesGcmEnvelope> {
        let key_uri = key_uri
            .strip_prefix(GCP_PREFIX)
            .ok_or_else(|| anyhow!("key uri must start with {}", GCP_PREFIX))?;
        Ok(AesGcmEnvelope {
            kms_client: self.create_kms_client(),
            key_uri: key_uri.into(),
            associated_data,
            base64_encoded,
        })
    }

    fn create_kms_client(
        &self,
    ) -> KeyManagementServiceClient<
        InterceptedService<tonic::transport::Channel, AuthorizationHeaderInterceptor>,
    > {
        KeyManagementServiceClient::with_interceptor(
            self.channel.clone(),
            self.authorization_header_interceptor.clone(),
        )
    }
}

/// Reimplements some logic from tink-rust (https://github.com/project-oak/tink-rust) because the library doesn't
/// work with async code.
pub struct AesGcmEnvelope {
    kms_client: KeyManagementServiceClient<
        InterceptedService<tonic::transport::Channel, AuthorizationHeaderInterceptor>,
    >,
    key_uri: String,
    associated_data: Vec<u8>,
    base64_encoded: bool,
}

impl AesGcmEnvelope {
    // TODO: implement encrypt

    pub async fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        let ciphertext_decoded = if self.base64_encoded {
            Some(base64::engine::general_purpose::STANDARD.decode(ciphertext)?)
        } else {
            None
        };

        let ciphertext = ciphertext_decoded
            .as_ref()
            .map(Vec::as_slice)
            .unwrap_or(ciphertext);

        let enc_dek_len = u32::from_be_bytes(ciphertext[..4].try_into()?) as usize;
        let ciphertext = &ciphertext[4..];

        let encrypted_dek = &ciphertext[..enc_dek_len];
        let payload = &ciphertext[enc_dek_len..];

        let decrypt_req = proto::DecryptRequest {
            name: self.key_uri.clone(),
            ciphertext: encrypted_dek.to_vec(),
            ..Default::default()
        };

        let mut client_clone = self.kms_client.clone();

        let decrypt_res = client_clone.decrypt(decrypt_req).await?.into_inner();

        let data_encryption_key = AesGcmKey::decode(decrypt_res.plaintext.as_slice())?;
        let aes_gcm = AesGcm::new(&data_encryption_key.key_value)
            .map_err(|e| anyhow!("failed initializing AesGcm key object: {:#?}", e))?;

        Ok(aes_gcm
            .decrypt(payload, &self.associated_data)
            .map_err(|e| anyhow!("failed decrypting payload: {:#?}", e))?)
    }
}
