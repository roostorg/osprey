use std::sync::Arc;

use http::header;
use tonic::{
    metadata::{errors::InvalidMetadataValue, Ascii, MetadataValue},
    service::Interceptor,
};

/// Indicates that the implementer can provide auth tokens for the auth headers
/// e.g. `Bearer abc.1231324dfasd`
pub trait AuthorizationHeaderProvider: Send + Sync + 'static {
    fn get_auth_metadata(&self) -> Result<MetadataValue<Ascii>, InvalidMetadataValue>;
}

#[derive(Clone)]
pub struct AuthorizationHeaderInterceptor {
    access_token_provider: Option<Arc<dyn AuthorizationHeaderProvider>>,
}

impl AuthorizationHeaderInterceptor {
    pub fn with_access_token_provider<T>(access_token_provider: T) -> Self
    where
        T: AuthorizationHeaderProvider,
    {
        Self {
            access_token_provider: Some(Arc::new(access_token_provider)),
        }
    }

    pub fn without_access_token_provider() -> Self {
        Self {
            access_token_provider: None,
        }
    }
}

impl Interceptor for AuthorizationHeaderInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(access_token_provider) = &self.access_token_provider {
            match access_token_provider.get_auth_metadata() {
                Ok(authorization_header) => {
                    request
                        .metadata_mut()
                        .insert(header::AUTHORIZATION.as_str(), authorization_header);
                }
                Err(err) => {
                    tracing::warn!("Failed to set authorization header: {}", err);
                }
            }
        };
        Ok(request)
    }
}
