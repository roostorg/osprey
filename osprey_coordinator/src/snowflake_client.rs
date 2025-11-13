use serde::{Deserialize, Serialize};
use thiserror::Error;

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
        // osprey-snowflake api spec: https://github.com/ayubun/snowflake-id-worker?tab=readme-ov-file#api-spec
        let mut snowflake_response: Vec<u64> = self
            .reqwest_client
            .post(format!("{}/generate", self.snowflake_api_endpoint))
            .json(&SnowflakeRequest { count: 1 })
            .send()
            .await?
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
