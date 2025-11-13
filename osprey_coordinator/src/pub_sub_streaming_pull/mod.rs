//! To get started, check out [`StreamingPullManager::new`].
//!
//! # Example Usage
//!
//! For examples, see the `examples` directory in this crate.
#![warn(unused)]

mod ack_id;
mod flow_control;
mod message;
mod message_ack_queue;
mod message_handler;
mod messages_in_flight;
mod messages_on_hold;
mod metrics;
mod streaming_pull_manager;
mod streaming_pull_manager_handle;

pub(crate) use crate::pub_sub_streaming_pull::ack_id::AckId;
pub(crate) use crate::pub_sub_streaming_pull::message_ack_queue::{AckOrNack, MessageAckQueue};
pub(crate) use crate::pub_sub_streaming_pull::messages_in_flight::MessagesInFlight;
pub(crate) use crate::pub_sub_streaming_pull::messages_on_hold::MessagesOnHold;
pub(crate) use crate::pub_sub_streaming_pull::metrics::StreamingPullManagerMetrics;
pub(crate) use crate::pub_sub_streaming_pull::streaming_pull_manager::StreamingPullManagerMessage;

pub use crate::pub_sub_streaming_pull::flow_control::FlowControl;
pub use crate::pub_sub_streaming_pull::message::{DetachedMessage, Message};
pub use crate::pub_sub_streaming_pull::message_handler::{
    MessageHandler, SpawnTaskPerMessageHandler,
};
pub use crate::pub_sub_streaming_pull::streaming_pull_manager::StreamingPullManager;
pub use crate::pub_sub_streaming_pull::streaming_pull_manager_handle::StreamingPullManagerHandle;
