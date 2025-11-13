use std::{borrow::Borrow, collections::HashMap, fmt, sync::Arc, time::Instant};

use prost_types::Timestamp;
use tokio::sync::mpsc::UnboundedSender;

use crate::gcloud::google::pubsub::v1::PubsubMessage;

use crate::pub_sub_streaming_pull::{AckId, AckOrNack, StreamingPullManagerMessage};

/// The message ack guard is used to ensure that when a message is dropped without being acked or nacked, it is nacked.
///
/// Acknowledgement of a message is an explicit action that must be invoked via [`MessageAckGuard::ack`].
///
/// This is only exposed when you call [`Message::into_inner`].
#[must_use]
pub struct MessageAckGuard {
    state: MessageAckGuardState,
}

enum MessageAckGuardState {
    Unacked {
        ack_id: AckId,
        received_at: Instant,
        sender: UnboundedSender<StreamingPullManagerMessage>,
    },
    Acked,
}

impl MessageAckGuard {
    pub(crate) fn new(
        sender: UnboundedSender<StreamingPullManagerMessage>,
        received_at: Instant,
        ack_id: AckId,
    ) -> MessageAckGuard {
        Self {
            state: MessageAckGuardState::Unacked {
                ack_id,
                received_at,
                sender,
            },
        }
    }

    /// See [`Message::ack`] for a description on what acking a message means.
    pub fn ack(mut self) {
        self.do_send(AckOrNack::Ack);
    }

    /// See [`Message::nack`] for a description on what nacking a message means.
    pub fn nack(mut self) {
        self.do_send(AckOrNack::Nack);
    }

    /// An internal method to mark that the message was acked elsewhere within the streaming pull manager.
    pub(crate) fn mark_acked_elsewhere(mut self) -> AckId {
        let ack_id = self.ack_id();
        self.state = MessageAckGuardState::Acked;
        ack_id
    }

    /// Sends an AckOrNack message to the streaming pull manager.
    fn do_send(&mut self, f: impl FnOnce(AckId) -> AckOrNack) {
        if let MessageAckGuardState::Unacked {
            ack_id,
            sender,
            received_at,
        } = std::mem::replace(&mut self.state, MessageAckGuardState::Acked)
        {
            sender
                .send(StreamingPullManagerMessage::AckOrNack {
                    ack_or_nack: f(ack_id),
                    latency: received_at.elapsed(),
                })
                .ok();
        }
    }

    fn ack_id(&self) -> AckId {
        match &self.state {
            MessageAckGuardState::Unacked { ack_id, .. } => *ack_id,
            MessageAckGuardState::Acked => unreachable!(
                "invariant: ack_id should never be called after message has been ack or nacked."
            ),
        }
    }
}

impl Drop for MessageAckGuard {
    fn drop(&mut self) {
        self.do_send(AckOrNack::Nack);
    }
}

/// A wrapper class for a pub-sub message, which automatically will nack the message if dropped.
///
/// In order to mark the message as processed, it is imperative to call [`Message::ack`], otherwise
/// the message will be re-delivered.
#[must_use]
pub struct Message {
    inner_message: DetachedMessage,
    guard: MessageAckGuard,
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Message")
            .field("message_id", &self.message_id)
            .finish()
    }
}

impl std::ops::Deref for Message {
    type Target = DetachedMessage;

    fn deref(&self) -> &Self::Target {
        &self.inner_message
    }
}

impl Message {
    pub(crate) fn new(inner_message: DetachedMessage, guard: MessageAckGuard) -> Self {
        Self {
            inner_message,
            guard,
        }
    }

    pub(crate) fn ack_id(&self) -> AckId {
        self.guard.ack_id()
    }

    /// Acknowledges the message, marking it as processed. After acknowledgement, a message should no longer be
    /// delivered to another subscriber on this subscription. However, this is not a guarantee, as pub-sub is at-least
    /// once delivery, not exactly-once.
    pub fn ack(self) -> DetachedMessage {
        self.guard.ack();
        self.inner_message
    }

    /// Nacks the message, marking it as not processed. When a message is nacked, it will be re-delivered to another
    /// subscriber. Often times immediately.
    pub fn nack(self) -> DetachedMessage {
        self.guard.nack();
        self.inner_message
    }

    /// Converts this message into the [`DetachedMessage`] which will let you take ownership of the various fields within
    /// the message. Also, returns the inner [`MessageAckGuard`] which you can use to ack/nack the message.
    pub fn into_inner(self) -> (DetachedMessage, MessageAckGuard) {
        (self.inner_message, self.guard)
    }
}

/// A message that has been detached from the [`crate::StreamingPullManager`] which sent it.
///
/// Obtained using [`Message::into_inner`], this struct has no special drop behavior.
#[derive(Debug, Clone)]
pub struct DetachedMessage {
    pub data: Vec<u8>,
    pub ordering_key: Option<Arc<str>>,
    pub attributes: HashMap<String, String>,
    pub message_id: String,
    delivery_attempt: u32,
    publish_time: Timestamp,
    size: usize,
}

impl DetachedMessage {
    pub(crate) fn new(m: PubsubMessage, delivery_attempt: u32) -> Self {
        let size = m.data.len()
            + m.message_id.len()
            + m.ordering_key.len()
            + m.attributes
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>();
        Self {
            size,
            data: m.data,
            ordering_key: if m.ordering_key.is_empty() {
                None
            } else {
                Some(m.ordering_key.into())
            },
            attributes: m.attributes,
            message_id: m.message_id,
            publish_time: m
                .publish_time
                .expect("invariant: message from pub-sub did not have a timestamp"),
            delivery_attempt,
        }
    }

    /// The "size" of the message, as used for flow control purposes. This is NOT the length of the data field, but
    /// sums up all the strings in the message to compute the size.
    pub(crate) fn size(&self) -> usize {
        self.size
    }

    /// The message data field. If this field is empty, the message must contain at least one attribute.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Attempts to decode the data as a given [`prost::Message`].
    pub fn decode_data<T: prost::Message + Default>(&self) -> Result<T, prost::DecodeError> {
        T::decode(self.data())
    }

    /// If non-empty, identifies related messages for which publish order should be respected.
    /// If a Subscription has enable_message_ordering set to true, messages published with the same non-empty
    /// ordering_key value will be delivered to subscribers in the order in which they are received by the Pub/Sub
    /// system. All PubsubMessages published in a given PublishRequest must specify the same ordering_key value.
    pub fn ordering_key(&self) -> Option<&str> {
        self.ordering_key.as_deref()
    }

    /// Attributes for this message. If this field is empty, the message must contain non-empty data.
    pub fn attributes(&self) -> &HashMap<String, String> {
        &self.attributes
    }

    /// Retrieves a given attribute.
    pub fn get_attribute<K>(&self, key: K) -> Option<&str>
    where
        String: Borrow<K>,
        K: std::hash::Hash + Eq,
    {
        self.attributes.get(&key).map(|x| x.as_ref())
    }

    /// ID of this message, assigned by the server when the message is published. Guaranteed to be unique within the
    /// topic.
    pub fn message_id(&self) -> &str {
        &self.message_id
    }

    /// The approximate number of times that Cloud Pub/Sub has attempted to deliver the associated message to a
    /// subscriber.
    ///
    /// More precisely, this is 1 + (number of NACKs) + (number of ack_deadline exceeds) for this message.
    ///
    /// A NACK is any call to ModifyAckDeadline with a 0 deadline. An ack_deadline exceeds event is whenever a message
    /// is not acknowledged within .
    ///
    /// Upon the first delivery of a given message, delivery_attempt will have a value of 1. The value is calculated at
    /// best effort and is approximate. If a DeadLetterPolicy is not set on the subscription, this will be 0.
    pub fn delivery_attempt(&self) -> u32 {
        self.delivery_attempt
    }

    /// The time at which the message received by the pub-sub server.
    ///
    /// It is important to note that the publish timestamp is generated when the server receives
    /// the message. Thus, it is not a good representation of "end to end" latency of how long a
    /// message takes to flow through through the system and be received by a subscriber, as
    /// there is latency between when the message is enqueued for publishing on the publisher end,
    /// and when that publish request is flushed and reaches the server (which is when the timestamp
    /// is generated.)
    ///
    /// If you require knowing the end-to-end latency of when a message was created, and when it was
    /// received, that must be stored out of bounds, either in the message data, or attributes.
    pub fn publish_time(&self) -> Timestamp {
        self.publish_time.clone()
    }
}
