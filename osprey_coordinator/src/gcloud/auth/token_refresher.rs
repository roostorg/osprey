use crate::tokio_utils::AbortOnDrop;
use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use goauth::{auth::JwtClaims, credentials::Credentials};
use smpl_jwt::Jwt;
use std::{
    cmp::max,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use tonic::metadata::{errors::InvalidMetadataValue, Ascii, MetadataValue};
use which::which;

use crate::gcloud::auth::Token;
use crate::gcloud::auth::{AuthorizationHeaderProvider, Scope};

#[async_trait]
pub trait TokenRefreshFn: Sync {
    async fn get_token(&self) -> Result<Token>;
}

#[derive(Clone)]
pub struct TokenRefresher<T> {
    token: Token,
    refresher: T,
}

/// A token refresher for using the gcloud command line to retrieve authentication credentials.
/// This shouldn't be used in production, but is very useful for testing things in development
pub(crate) struct GCloudAuth;

#[async_trait]
impl TokenRefreshFn for GCloudAuth {
    async fn get_token(&self) -> Result<Token> {
        let gcloud_path = which("gcloud")?;
        let output = tokio::process::Command::new(gcloud_path)
            .args(["auth", "print-access-token", "--quiet"])
            .output()
            .await?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let str_token = stdout.trim();
        let token = Token::new(
            str_token.to_owned(),
            12 * 60 * 60, // 12 hours
            "Bearer".into(),
            Instant::now(),
        );

        tracing::info!("Token expires in {} seconds", token.ttl_secs());
        Ok(token)
    }
}

impl TokenRefresher<GCloudAuth> {
    pub async fn with_gcloud_auth() -> Result<Self> {
        let initial = GCloudAuth.get_token().await?;
        Ok(TokenRefresher::new(initial, GCloudAuth))
    }
}

pub(crate) struct ApplicationCredentials {
    credentials: Credentials,
    scope: Scope,
}

#[async_trait]
impl TokenRefreshFn for ApplicationCredentials {
    async fn get_token(&self) -> Result<Token> {
        tracing::info!("Requesting token for {:?} scope", self.scope);
        let credentials = &self.credentials;

        let claims = JwtClaims::new(
            credentials.iss(),
            &self.scope,
            credentials.token_uri(),
            None,
            None,
        );
        let jwt = Jwt::new(claims, credentials.rsa_key().unwrap(), None);

        let token = goauth::get_token(&jwt, credentials)
            .await
            .context("Failed to refresh access token")?;
        let token = Token::from_goauth(token, Instant::now());

        tracing::info!("Token expires in {} seconds", token.ttl_secs());
        Ok(token)
    }
}

impl TokenRefresher<ApplicationCredentials> {
    fn load_credentials() -> Result<Credentials> {
        let credentials_file = std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
            .context("GOOGLE_APPLICATION_CREDENTIALS environment variable not found")?;

        Credentials::from_file(&credentials_file)
            .with_context(|| format!("Failed to read GCP credentials from {credentials_file}"))
    }

    /// Helper to create a client using application-credentials contained with a slice containing json
    pub async fn with_json_credentials(scope: Scope, data: &[u8]) -> Result<Self> {
        let credentials: Credentials = serde_json::from_slice(data)?;
        Self::with_credentials(scope, credentials).await
    }

    async fn with_credentials(scope: Scope, credentials: Credentials) -> Result<Self> {
        credentials.rsa_key().context("Invalid RSA key")?;
        let refresher = ApplicationCredentials { credentials, scope };

        let access_token_refresher = Self {
            token: refresher
                .get_token()
                .await
                .context("Initial credential load")?,
            refresher,
        };
        Ok(access_token_refresher)
    }

    pub async fn with_local_credentials(scope: Scope) -> Result<Self> {
        let credentials = Self::load_credentials()?;
        Self::with_credentials(scope, credentials).await
    }
}

impl<T> TokenRefresher<T>
where
    T: TokenRefreshFn + Send + 'static,
{
    pub fn new(token: Token, refresher: T) -> Self {
        Self { token, refresher }
    }

    /// Starts a background task to refresh the token every `refresh_period`.
    /// This returns a `TokenHandler` that can be cloned and sent to clients; once
    /// all the references are dropped this task is stopped.
    pub fn spawn_refresher(mut self, refresh_period: Duration) -> TokenHandle {
        let auth_header_value = Arc::new(ArcSwap::from(Arc::new(self.token.to_header_rep())));
        let refresher_auth_header_value = auth_header_value.clone();

        let join_handle = tokio::spawn(async move {
            tracing::info!("AccessTokenRefresher refresher started");
            loop {
                let expires_in = self.token.ttl_secs();
                // Give ourselves some time before it actually expires
                let expires_in = expires_in.saturating_sub(60);

                let min_refresh_period = if refresh_period.as_secs() < expires_in {
                    refresh_period
                } else {
                    tracing::warn!(
                        "Token refresh interval was shorter than token expiration time: {}s",
                        expires_in
                    );
                    // Sometimes "refreshing" the token doesn't actually refresh anything (we just pull the token from
                    // a get request against GCP).
                    // Just in case GCP is refreshing automatically at the e.g. 30 seconds to expire mark, we should not
                    // spin here since expires_in will be 0 for 60 seconds before the token expires.
                    Duration::from_secs(max(expires_in, 1))
                };

                self.refresh(&refresher_auth_header_value).await;
                tokio::time::sleep(min_refresh_period).await;
            }
        });

        TokenHandle {
            auth_header_value,
            _token_refresher_handle: AbortOnDrop::new(join_handle),
        }
    }

    /// Fetch a new instance of the [Token] and update the stored value
    async fn refresh(&mut self, access_token_header_value: &Arc<ArcSwap<String>>) {
        if self.token.created_at.elapsed().as_secs() < (self.token.expires_in_secs - 3) / 2 {
            // No need to refresh token since we will wake again before token expires
            return;
        }

        tracing::debug!("Refreshing token");
        let new_token = self.refresher.get_token().await;
        match new_token {
            Ok(new_token) => {
                access_token_header_value.store(Arc::new(new_token.to_header_rep()));
                self.token = new_token;
            }
            Err(err) => tracing::warn!("{}", err),
        }
    }
}

pub struct TokenHandle {
    pub(crate) auth_header_value: Arc<ArcSwap<String>>,
    /// Held to ensure that the refresh task lives only as long as this client exists
    pub(crate) _token_refresher_handle: AbortOnDrop<()>,
}

impl AuthorizationHeaderProvider for TokenHandle {
    fn get_auth_metadata(&self) -> Result<MetadataValue<Ascii>, InvalidMetadataValue> {
        let value = self.auth_header_value.load();
        FromStr::from_str(&value)
    }
}
