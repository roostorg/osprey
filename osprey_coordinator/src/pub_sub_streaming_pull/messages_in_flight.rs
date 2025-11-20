use std::sync::Arc;

use fxhash::FxHashMap;

use crate::pub_sub_streaming_pull::ack_id::AckId;

struct MessageInFlight {
    ordering_key: Option<Arc<str>>,
    size_in_bytes: usize,
}

#[derive(Default)]
pub(crate) struct MessagesInFlight {
    messages_in_flight: FxHashMap<AckId, MessageInFlight>,
    bytes_in_flight: usize,
}

impl MessagesInFlight {
    pub(crate) fn bytes_in_flight(&self) -> usize {
        self.bytes_in_flight
    }

    pub(crate) fn len(&self) -> usize {
        self.messages_in_flight.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.messages_in_flight.is_empty()
    }

    pub(crate) fn remove(&mut self, ack_id: &AckId) -> Option<Option<Arc<str>>> {
        let removed = self.messages_in_flight.remove(ack_id)?;
        self.bytes_in_flight -= removed.size_in_bytes;
        Some(removed.ordering_key)
    }

    pub(crate) fn insert(&mut self, message: &crate::pub_sub_streaming_pull::Message) {
        let size_in_bytes = message.size();
        let message_in_flight = MessageInFlight {
            ordering_key: message.ordering_key.clone(),
            size_in_bytes,
        };

        if let Some(prev_in_flight) = self
            .messages_in_flight
            .insert(message.ack_id(), message_in_flight)
        {
            self.bytes_in_flight -= prev_in_flight.size_in_bytes;
        }

        self.bytes_in_flight += size_in_bytes;
    }
}
