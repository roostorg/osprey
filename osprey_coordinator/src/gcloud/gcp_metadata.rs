use anyhow::{Context, Result};
use async_trait::async_trait;
use reqwest::Client;

use crate::gcloud::auth::{Token, TokenRefreshFn, TokenRefresher};

/// Taken from https://cloud.google.com/appengine/docs/standard/java/accessing-instance-metadata
const METADATA_URL: &str = "http://metadata.google.internal/computeMetadata/v1";
static APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

/// Provides helpers to get data about a GCP instance. This is done through accessing a http endpoint
/// that contains metadata about the current service.
///
/// See https://cloud.google.com/appengine/docs/standard/java/accessing-instance-metadata
#[derive(Debug, Clone)]
pub struct GCPMetadataClient {
    client: Client,
    service_account: String,
}

impl GCPMetadataClient {
    pub fn new(service_account: String) -> Result<Self> {
        let client = Client::builder().user_agent(APP_USER_AGENT).build()?;
        Ok(Self {
            client,
            service_account,
        })
    }

    pub async fn get_access_token(&self) -> Result<Token> {
        let url = format!(
            "{}/instance/service-accounts/{}/token",
            METADATA_URL, self.service_account
        );

        /*
         We should see something like this:
         {
            "access_token":"ya29.AHES6ZRN3-HlhAPya30GnW_bHSb_QtAS08i85nHq39HE3C2LTrCARA",
            "expires_in":3599,
            "token_type":"Bearer"
         }
        */
        let access_token = self
            .client
            .get(url)
            .header("Metadata-Flavor", "Google")
            .send()
            .await?
            .json::<Token>()
            .await?;
        Ok(access_token)
    }
}

#[async_trait]
impl TokenRefreshFn for GCPMetadataClient {
    async fn get_token(&self) -> Result<Token> {
        self.get_access_token().await
    }
}

impl TokenRefresher<GCPMetadataClient> {
    pub async fn with_metadata_client(client: GCPMetadataClient) -> Result<Self> {
        Ok(Self::new(
            client
                .get_access_token()
                .await
                .context("Initial credential load")?,
            client,
        ))
    }
}
