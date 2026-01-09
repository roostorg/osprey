use etcd::kv::{self, WatchError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EtcdError {
    #[error("API error. index={} error={}", index, error)]
    API { index: u64, error: etcd::Error },
    #[error("Key not found. index={} error={}", index, error)]
    KeyNotFound { index: u64, error: etcd::Error },
    #[error("Key already exists. index={} error={}", index, error)]
    KeyAlreadyExists { index: u64, error: etcd::Error },
    #[error("Watch error. error={}", error)]
    Watch { error: kv::WatchError },
    #[error("Serialization error. error={}", error)]
    Serialize { error: anyhow::Error },
    #[error("The response had no index.")]
    NoIndex,
    #[error("IoError. error={}", error)]
    IoError {
        #[from]
        error: std::io::Error,
    },
    #[error("Etcd error. error={}", error)]
    Unknown { error: etcd::Error },
}

impl EtcdError {
    pub fn index(&self) -> u64 {
        match self {
            EtcdError::API { index, .. } => *index,
            _ => 0,
        }
    }
}

impl From<Vec<etcd::Error>> for EtcdError {
    fn from(errors: Vec<etcd::Error>) -> EtcdError {
        let error = errors
            .into_iter()
            .next()
            .expect("invariant: errors must not be empty");

        match error {
            etcd::Error::Api(error) => {
                if error.error_code == 100 {
                    EtcdError::KeyNotFound {
                        index: error.index,
                        error: etcd::Error::Api(error),
                    }
                } else if error.error_code == 105 {
                    EtcdError::KeyAlreadyExists {
                        index: error.index,
                        error: etcd::Error::Api(error),
                    }
                } else {
                    EtcdError::API {
                        index: error.index,
                        error: etcd::Error::Api(error),
                    }
                }
            }
            other => EtcdError::Unknown { error: other },
        }
    }
}

impl From<kv::WatchError> for EtcdError {
    fn from(error: WatchError) -> EtcdError {
        match error {
            WatchError::Other(errors) => EtcdError::from(errors),
            error => EtcdError::Watch { error },
        }
    }
}
