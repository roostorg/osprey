use std::time::Duration;

pub mod auth;
pub mod gcp_metadata;
pub mod grpc;
mod root_ca_certificate;

pub mod kms;
pub mod pubsub;

/// Common options for creating authenticated connections to Google Cloud APIs
pub struct ConnectionOptions {
    /// Request timeout
    pub timeout: Duration,

    /// Duration to wait before refreshing the access token used for talking to cloud APIs
    pub token_refresh_period: Duration,
}

impl Default for ConnectionOptions {
    fn default() -> ConnectionOptions {
        Self {
            timeout: Duration::from_secs(5),
            token_refresh_period: Duration::from_secs(240000), // 4 Hours
        }
    }
}

pub mod google {
    pub mod api {
        tonic::include_proto!("google.api");
    }

    pub mod iam {
        pub mod v1 {
            tonic::include_proto!("google.iam.v1");
        }
    }

    pub mod longrunning {
        tonic::include_proto!("google.longrunning");
    }

    pub mod rpc {
        tonic::include_proto!("google.rpc");
    }
    pub mod r#type {
        tonic::include_proto!("google.r#type");
    }

    pub mod crypto {
        pub mod tink {
            tonic::include_proto!("google.crypto.tink");
        }
    }

    pub mod pubsub {
        pub mod v1 {
            tonic::include_proto!("google.pubsub.v1");
        }
    }

    pub mod kms {
        pub mod v1 {
            tonic::include_proto!("google.cloud.kms.v1");
        }
    }
}
