use anyhow::Result;
use async_trait::async_trait;
use prost_types::Timestamp;
use std::collections::HashMap;
use tokio::time::Duration as TokioDuration;

#[derive(Clone)]
pub struct ConsumerConfig {
    pub max_time_to_send_to_async_queue: TokioDuration,
    pub max_acking_receiver_wait_time: TokioDuration,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            max_time_to_send_to_async_queue: TokioDuration::from_millis(
                std::env::var("MAX_TIME_TO_SEND_TO_ASYNC_QUEUE_MS")
                    .unwrap_or("500".to_string())
                    .parse::<u64>()
                    .unwrap(),
            ),
            max_acking_receiver_wait_time: TokioDuration::from_millis(
                std::env::var("MAX_ACKING_RECEIVER_WAIT_TIME_MS")
                    .unwrap_or("60000".to_string())
                    .parse::<u64>()
                    .unwrap(),
            ),
        }
    }
}

pub trait ConsumerMessage {
    fn data(&self) -> &[u8];
    fn attributes(&self) -> &HashMap<String, String>;
    fn timestamp(&self) -> Timestamp;
    fn id(&self) -> String;
}

#[async_trait]
pub trait MessageConsumer: Send {
    type Message: ConsumerMessage;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn receive(&mut self) -> Result<Self::Message, Self::Error>;

    async fn ack(&self, message: &Self::Message) -> Result<(), Self::Error>;

    async fn nack(&self, message: &Self::Message) -> Result<(), Self::Error>;
}
