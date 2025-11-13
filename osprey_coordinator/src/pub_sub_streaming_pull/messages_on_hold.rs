use std::{collections::VecDeque, sync::Arc};

use fxhash::FxHashMap;

use crate::pub_sub_streaming_pull::{ack_id::AckId, Message};

/// This struct exists for the sole purpose of reminding us to call `unwrap_message` when we remove the message
/// from `MessagesOnHold`.
struct WrappedMessage(Message);

/// MessageOnHold is responsible for buffering messages sent from the pub-sub server,
/// holding onto the messages if there are too many in-flight, or if exact-ordering is
/// enabled, buffering messages within a given ordering_key, until a message for the
/// ordering key has completed.
#[derive(Default)]
pub(crate) struct MessagesOnHold {
    /// The number of messages on hold. This is the sum of the length of `message_on_hold`
    /// and the sum of the lengths of the values in `pending_ordered_messages`.
    len: usize,
    /// The size in bytes of the messages on hold, computed using [`Message::size`], computed
    /// ontop of messages_on_hold, and also pending_ordered_messages.
    size_in_bytes: usize,
    /// Messages that ready to be processed by `pop`.
    ready_messages: VecDeque<WrappedMessage>,
    /// Messages that are on hold for a given ordering key.
    pending_ordered_messages: FxHashMap<Arc<str>, VecDeque<WrappedMessage>>,
}

impl MessagesOnHold {
    /// The number of messages on hold.
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// The size in bytes of the messages on hold, computed using [`Message::size`]
    pub(crate) fn size_in_bytes(&self) -> usize {
        self.size_in_bytes
    }

    /// Returns how many messages on hold are ready to be handled.
    pub(crate) fn num_ready_messages(&self) -> usize {
        self.ready_messages.len()
    }

    /// Pushes a message into the relevant messages on hold queue.
    pub(crate) fn push(&mut self, message: Message) {
        let queue_target = match &message.ordering_key {
            // If the message has an ordering key, insert it into messages on hold if there is no queue for the ordering
            // key and create a queue. Otherwise if the queue exists, insert the message into the respective queue.
            Some(ordering_key) => {
                match self.pending_ordered_messages.get_mut(ordering_key) {
                    Some(queue) => queue,
                    None => {
                        // We create a queue for this ording key, so that if we push another message
                        // with this given ordering key, it will be buffered within the queue, until
                        // `activate_ordering_key` is called. The presence of the queue means that we have
                        // a message this ordering key in the messages on hold queue.
                        self.pending_ordered_messages
                            .entry(ordering_key.clone())
                            .or_default();

                        &mut self.ready_messages
                    }
                }
            }
            None => &mut self.ready_messages,
        };

        self.len += 1;
        self.size_in_bytes += message.size();
        queue_target.push_back(WrappedMessage(message));
    }

    /// Unwraps a message, decrementing the length and size of the MessagesOnHold struct.
    fn unwrap_message(&mut self, message: WrappedMessage) -> Message {
        let message = message.0;

        self.len -= 1;
        self.size_in_bytes -= message.size();

        message
    }

    /// Gets a message from the on-hold queue. A message with an ordering
    /// key wont be returned if there's another message with the same key in
    /// flight.
    #[inline(always)]
    pub(crate) fn pop_ready(&mut self) -> Option<Message> {
        self.ready_messages
            .pop_front()
            .map(|wrapped_message| self.unwrap_message(wrapped_message))
    }

    /// Internally moves an enqueued item from the front of given ordering key queue to the ready queue.
    pub(crate) fn activate_ordering_key(&mut self, ordering_key: &str) {
        if let Some(wrapped_message) = self.get_next_for_ordering_key(ordering_key) {
            self.ready_messages.push_front(wrapped_message);
        }
    }

    /// Pops the next message for a given ordering key, removing the queue for a given ordering key,
    /// if it is empty.
    fn get_next_for_ordering_key(&mut self, ordering_key: &str) -> Option<WrappedMessage> {
        let queue_for_key = self.pending_ordered_messages.get_mut(ordering_key)?;
        match queue_for_key.pop_front() {
            // We have a message in the queue, return it and still preserve the queue.
            Some(message) => Some(message),
            // The queue is empty, we can clean it up now.
            None => {
                self.pending_ordered_messages.remove(ordering_key);
                None
            }
        }
    }

    /// Removes all messages from the messages on hold,
    pub(crate) fn drain_and_collect_all_ack_ids(&mut self) -> Vec<AckId> {
        let mut ack_ids = Vec::with_capacity(self.len());

        for message in self.ready_messages.drain(..) {
            let (_, guard) = message.0.into_inner();
            ack_ids.push(guard.mark_acked_elsewhere());
        }

        for (_ordering_key, mut messages) in self.pending_ordered_messages.drain() {
            for message in messages.drain(..) {
                let (_, guard) = message.0.into_inner();
                ack_ids.push(guard.mark_acked_elsewhere());
            }
        }

        // Reset these values, we should now be fully empty!
        self.len = 0;
        self.size_in_bytes = 0;

        debug_assert!(self.ready_messages.is_empty());
        debug_assert!(self.pending_ordered_messages.is_empty());

        ack_ids
    }
}
