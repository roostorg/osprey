use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;
use tokio::time::sleep;

const MAX_RETRIES: usize = 3;
const RETRY_BASE_DELAY_MILLIS: u64 = 50;

#[derive(Debug, Serialize, Deserialize)]
struct SnowflakeRequest {
    count: u32,
}

#[derive(Error, Debug)]
pub enum SnowflakeClientError {
    #[error(transparent)]
    IdGenerateError(#[from] reqwest::Error),

    #[error("no snowflake id returned from snowflake service")]
    NoIdGeneratedError,
}

impl SnowflakeClientError {
    fn is_retryable(&self) -> bool {
        let Self::IdGenerateError(error) = self else {
            return false;
        };
        if let Some(status) = error.status() {
            return status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error();
        }
        error.is_connect() || error.is_timeout() || error.is_body() || error.is_request()
    }
}

fn retry_delay(attempt: usize) -> Duration {
    Duration::from_millis((attempt + 1).pow(2) as u64 * RETRY_BASE_DELAY_MILLIS)
}

pub struct SnowflakeClient {
    snowflake_api_endpoint: String,
    reqwest_client: reqwest::Client,
    // buffer: SnowflakeBuffer,
    // request_size: u32,
}

impl SnowflakeClient {
    pub fn new(snowflake_api_endpoint: String) -> Self {
        Self {
            snowflake_api_endpoint,
            reqwest_client: reqwest::Client::new(),
            // note: to get a buffer working without hindering performance, we'd need to figure
            // out a way to pass around the buffer without the dreaded Arc<Mutex<>>. because i dont wanna
            // figure that out, and it'd be definitely overkill for now, i'm just gonna Arc<> and make the
            // client only perform single id requests.
            //
            // buffer: SnowflakeBuffer::new(),
            // // todo: it would be cool if this was calculated based on a rolling window
            // // of the number of ids requested in the last 500ms or so . . but that's way
            // // too overengineered for now lol
            // request_size: 1,
        }
    }

    pub async fn generate_id(&self) -> Result<u64, SnowflakeClientError> {
        for attempt in 0..=MAX_RETRIES {
            match self.generate_id_once().await {
                Ok(id) => return Ok(id),
                Err(error) if attempt < MAX_RETRIES && error.is_retryable() => {
                    sleep(retry_delay(attempt)).await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!()
    }

    async fn generate_id_once(&self) -> Result<u64, SnowflakeClientError> {
        // osprey-snowflake api spec: https://github.com/ayubun/snowflake-id-worker?tab=readme-ov-file#api-spec
        let mut snowflake_response: Vec<u64> = self
            .reqwest_client
            .post(format!("{}/generate", self.snowflake_api_endpoint))
            .json(&SnowflakeRequest { count: 1 })
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let id = snowflake_response
            .pop()
            .ok_or(SnowflakeClientError::NoIdGeneratedError)?;

        Ok(id)
    }
}

// struct SnowflakeBuffer {
//     buffer: Vec<u64>,
//     last_buffer_fill: SystemTime,
//     buffer_ttl: Duration,
// }

// impl SnowflakeBuffer {
//     pub fn new() -> Self {
//         Self {
//             buffer: Vec::new(),
//             last_buffer_fill: UNIX_EPOCH,
//             // we don't want to use buffered ids older than 1 millisecond, since that
//             // is the precision of snowflake timestamps, and we want our snowflakes
//             // to be as close to the time of generation as possible.
//             buffer_ttl: Duration::from_millis(1),
//         }
//     }

//     /// returns the next id in the buffer, or `None` if the buffer
//     /// cannot provide an id (either empty or buffer is too old)
//     pub fn next_id(&mut self) -> Option<u64> {
//         if !self.buffer.is_empty() && self
//             .last_buffer_fill
//             .elapsed()
//             .unwrap_or_else(|_| {
//                 UNIX_EPOCH
//                     .elapsed()
//                     .expect("invariant: literally impossible")
//             })
//             .as_nanos()
//             > self.buffer_ttl.as_nanos()
//         {
//             self.buffer.clear();
//         }
//         self.buffer.pop()
//     }

//     /// fills the buffer with the given ids.
//     pub fn fill(&mut self, ids: Vec<u64>) {
//         self.buffer = ids;
//         self.last_buffer_fill = SystemTime::now();
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        task::JoinHandle,
    };

    enum TestResponse {
        Disconnect,
        Status(StatusCode, &'static str),
    }

    struct TestServer {
        endpoint: String,
        requests: Arc<AtomicUsize>,
        handle: JoinHandle<()>,
    }

    impl TestServer {
        async fn start(responses: Vec<TestResponse>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let requests = Arc::new(AtomicUsize::new(0));
            let request_counter = Arc::clone(&requests);
            let handle = tokio::spawn(async move {
                for response in responses {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    request_counter.fetch_add(1, Ordering::SeqCst);
                    match response {
                        TestResponse::Disconnect => continue,
                        TestResponse::Status(status, body) => {
                            let mut request = [0; 1024];
                            let _ = stream.read(&mut request).await;
                            let response = format!(
                                "HTTP/1.1 {} {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                                status.as_u16(),
                                status.canonical_reason().unwrap_or("unknown"),
                                body.len(),
                                body
                            );
                            stream.write_all(response.as_bytes()).await.unwrap();
                        }
                    }
                }
            });
            Self {
                endpoint: format!("http://{address}"),
                requests,
                handle,
            }
        }

        fn request_count(&self) -> usize {
            self.requests.load(Ordering::SeqCst)
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.handle.abort();
        }
    }

    #[tokio::test]
    async fn retries_rate_limit_response() {
        let server = TestServer::start(vec![
            TestResponse::Status(StatusCode::TOO_MANY_REQUESTS, "queue full"),
            TestResponse::Status(StatusCode::OK, "[123]"),
        ])
        .await;
        let client = SnowflakeClient::new(server.endpoint.clone());

        assert_eq!(client.generate_id().await.unwrap(), 123);
        assert_eq!(server.request_count(), 2);
    }

    #[tokio::test]
    async fn retries_server_error_response() {
        let server = TestServer::start(vec![
            TestResponse::Status(StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
            TestResponse::Status(StatusCode::OK, "[123]"),
        ])
        .await;
        let client = SnowflakeClient::new(server.endpoint.clone());

        assert_eq!(client.generate_id().await.unwrap(), 123);
        assert_eq!(server.request_count(), 2);
    }

    #[tokio::test]
    async fn retries_transport_error() {
        let server = TestServer::start(vec![
            TestResponse::Disconnect,
            TestResponse::Status(StatusCode::OK, "[123]"),
        ])
        .await;
        let client = SnowflakeClient::new(server.endpoint.clone());

        assert_eq!(client.generate_id().await.unwrap(), 123);
        assert_eq!(server.request_count(), 2);
    }

    #[tokio::test]
    async fn does_not_retry_client_error_response() {
        let server = TestServer::start(vec![
            TestResponse::Status(StatusCode::BAD_REQUEST, "bad request"),
            TestResponse::Status(StatusCode::OK, "[123]"),
        ])
        .await;
        let client = SnowflakeClient::new(server.endpoint.clone());

        let error = client.generate_id().await.unwrap_err();
        match error {
            SnowflakeClientError::IdGenerateError(source) => {
                assert_eq!(source.status(), Some(StatusCode::BAD_REQUEST));
            }
            SnowflakeClientError::NoIdGeneratedError => panic!("expected request error"),
        }
        assert_eq!(server.request_count(), 1);
    }

    #[tokio::test]
    async fn stops_after_four_transient_attempts() {
        let server = TestServer::start(vec![
            TestResponse::Status(StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
            TestResponse::Status(StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
            TestResponse::Status(StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
            TestResponse::Status(StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
        ])
        .await;
        let client = SnowflakeClient::new(server.endpoint.clone());

        assert!(client.generate_id().await.is_err());
        assert_eq!(server.request_count(), 4);
    }
}
