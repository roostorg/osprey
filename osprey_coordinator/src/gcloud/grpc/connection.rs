//! Wrappers around making an authenticated GRPC connection to a Google Cloud service.

use std::time::Duration;

use thiserror::Error;
use tonic::transport::ClientTlsConfig;

use crate::gcloud::{
    auth::{AuthorizationHeaderInterceptor, Scope, TokenRefresher},
    gcp_metadata::GCPMetadataClient,
    root_ca_certificate,
};

#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("AccessToken error: {0}")]
    AccessTokenError(anyhow::Error),

    #[error("Certificate error: {0}")]
    CertificateError(String),

    #[error("I/O Error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Invalid URI {0}")]
    InvalidUri(#[from] http::uri::InvalidUri),

    #[error("Transport error: {0}")]
    TransportError(#[from] tonic::transport::Error),
}

pub type Result<T> = std::result::Result<T, ConnectionError>;

#[derive(Clone)]
pub struct Connection {
    pub(crate) authorization_header_interceptor: AuthorizationHeaderInterceptor,
    pub(crate) channel: tonic::transport::Channel,
}

impl Connection {
    fn create_google_endpoint(domain: &str) -> Result<tonic::transport::Endpoint> {
        let endpoint = tonic::transport::Channel::from_shared(format!("https://{domain}"))?;
        let tls_config = ClientTlsConfig::new()
            .ca_certificate(root_ca_certificate::load().map_err(ConnectionError::CertificateError)?)
            .domain_name(domain);
        endpoint.tls_config(tls_config).map_err(|e| e.into())
    }

    pub async fn from_metadata_client(
        client: GCPMetadataClient,
        timeout: impl Into<Option<Duration>>,
        token_refresh_period: Duration,
        domain: &str,
    ) -> Result<Self> {
        let access_token_handle = TokenRefresher::with_metadata_client(client)
            .await
            .map_err(ConnectionError::AccessTokenError)?
            .spawn_refresher(token_refresh_period);

        let connection = Self {
            authorization_header_interceptor:
                AuthorizationHeaderInterceptor::with_access_token_provider(access_token_handle),
            channel: apply_timeout(Self::create_google_endpoint(domain)?, timeout.into()),
        };
        Ok(connection)
    }

    pub async fn from_json(
        timeout: impl Into<Option<Duration>>,
        token_refresh_period: Duration,
        domain: &str,
        ac_json: &[u8],
        scope: Scope,
    ) -> Result<Self> {
        let access_token_handle = TokenRefresher::with_json_credentials(scope, ac_json)
            .await
            .map_err(ConnectionError::AccessTokenError)?
            .spawn_refresher(token_refresh_period);

        let connection = Self {
            authorization_header_interceptor:
                AuthorizationHeaderInterceptor::with_access_token_provider(access_token_handle),
            channel: apply_timeout(Self::create_google_endpoint(domain)?, timeout.into()),
        };
        Ok(connection)
    }

    /// Create a new connection using gcloud cli authentication
    pub async fn new_with_gcloud(
        timeout: impl Into<Option<Duration>>,
        token_refresh_period: Duration,
        domain: &str,
    ) -> Result<Self> {
        let access_token_handle = TokenRefresher::with_gcloud_auth()
            .await
            .map_err(ConnectionError::AccessTokenError)?
            .spawn_refresher(token_refresh_period);

        let connection = Self {
            authorization_header_interceptor:
                AuthorizationHeaderInterceptor::with_access_token_provider(access_token_handle),
            channel: apply_timeout(Self::create_google_endpoint(domain)?, timeout.into()),
        };
        Ok(connection)
    }

    pub async fn new(
        timeout: impl Into<Option<Duration>>,
        token_refresh_period: Duration,
        domain: &str,
        scope: Scope,
    ) -> Result<Self> {
        let access_token_handle = TokenRefresher::with_local_credentials(scope)
            .await
            .map_err(ConnectionError::AccessTokenError)?
            .spawn_refresher(token_refresh_period);

        let connection = Self {
            authorization_header_interceptor:
                AuthorizationHeaderInterceptor::with_access_token_provider(access_token_handle),
            channel: apply_timeout(Self::create_google_endpoint(domain)?, timeout.into()),
        };
        Ok(connection)
    }

    pub fn new_no_auth(
        endpoint: tonic::transport::Endpoint,
        timeout: impl Into<Option<Duration>>,
    ) -> Self {
        Self {
            authorization_header_interceptor:
                AuthorizationHeaderInterceptor::without_access_token_provider(),
            channel: apply_timeout(endpoint, timeout.into()),
        }
    }
}

fn apply_timeout(
    endpoint: tonic::transport::Endpoint,
    timeout: Option<Duration>,
) -> tonic::transport::Channel {
    match timeout {
        Some(timeout) => endpoint.timeout(timeout),
        None => endpoint,
    }
    .connect_lazy()
}
