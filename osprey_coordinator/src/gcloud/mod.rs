pub mod auth;
pub mod gcp_metadata;
pub mod grpc;
mod root_ca_certificate;

pub mod kms;
pub mod pubsub;

// These modules contain generated prost and tonic code.
#[allow(clippy::all, dead_code)]
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
