use std::{
    collections::VecDeque,
    future::pending,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use fxhash::FxHashMap;
use rand::{thread_rng, Rng};
use tokio_util::time::{delay_queue::Key, DelayQueue};

use crate::pub_sub_streaming_pull::ack_id::AckId;

pub(crate) enum AckOrNack<T = AckId> {
    Ack(T),
    Nack(T),
}

impl<T> AckOrNack<T> {
    pub(crate) fn ack_id(&self) -> &T {
        match self {
            AckOrNack::Ack(ack_id) | AckOrNack::Nack(ack_id) => ack_id,
        }
    }

    pub(crate) fn map<R, F>(self, f: F) -> AckOrNack<R>
    where
        F: FnOnce(T) -> R,
    {
        match self {
            AckOrNack::Ack(ack_id) => AckOrNack::Ack(f(ack_id)),
            AckOrNack::Nack(ack_id) => AckOrNack::Nack(f(ack_id)),
        }
    }
}

struct MessageAckQueueItem<AckIdFromServerT = String> {
    /// The ack id as provided by pub-sub (which is mapped in [`MessageAckQueue::ack_id_mapping`]).
    ack_id_from_server: AckIdFromServerT,
    /// The key for the lease renewal timer ([`MessageAckQueue::lease_renewal_delay_queue`]).
    lease_delay_queue_key: Key,
    /// Instant in which the message was enqueued.
    enqueued_at: Instant,
}

/// The message ack queue is responsible for taking in acks or nacks via `enqueue_ack_or_nack`, and then
/// combining those acks into a chunk that is then provided via `next_chunk`.
pub(crate) struct MessageAckQueue<AckIdFromServer = String> {
    /// How big each chunk should be. If the number of enqueued acks exceeds this size,
    /// `next_chunk` will produce chunks capped at this size, until the pending acks are
    /// empty.
    max_chunk_size: usize,
    /// How long should we wait until our pending_acks queue fills up, till we flush an incomplete
    /// chunk.
    pending_ack_flush_interval: Duration,
    /// When we should perform the next flush (meaning, we will yield a partial chunk.)
    next_flush: Instant,
    /// The array of pending acks (our queue).
    current_chunk: Vec<AckOrNack<AckIdFromServer>>,
    /// Contains the "full" chunks, which have filled up to `max_chunk_size`.
    full_chunks: VecDeque<Vec<AckOrNack<AckIdFromServer>>>,
    /// A mapping of a ack id => [`MessageAckQueueItem`].
    ack_id_mapping: FxHashMap<AckId, MessageAckQueueItem<AckIdFromServer>>,
    /// The last ack id we dispensed, so we can get the next ack id.
    last_ack_id: AckId,
    /// The lease renewal delay queue, which tracks when we should renew leases for a given item.
    lease_renewal_delay_queue: DelayQueue<AckId>,
}

impl<AckIdFromServer> MessageAckQueue<AckIdFromServer>
where
    AckIdFromServer: Clone,
{
    pub(crate) fn new(
        flow_control_max_messages: usize,
        max_chunk_size: usize,
        pending_ack_flush_interval: Duration,
    ) -> Self {
        Self {
            max_chunk_size,
            pending_ack_flush_interval,
            next_flush: Instant::now() + pending_ack_flush_interval,
            current_chunk: Vec::with_capacity(max_chunk_size),
            full_chunks: VecDeque::with_capacity(flow_control_max_messages / max_chunk_size),
            ack_id_mapping: Default::default(),
            last_ack_id: AckId::new(),
            lease_renewal_delay_queue: DelayQueue::with_capacity(flow_control_max_messages),
        }
    }

    /// Returns the amount of pending acks/nacks that have yet to be flushed.
    pub(crate) fn len(&self) -> usize {
        self.current_chunk.len() + (self.full_chunks.len() * self.max_chunk_size)
    }

    /// Returns True if there are any pending acks/nacks that have yet to be flushed.
    pub(crate) fn is_empty(&self) -> bool {
        self.current_chunk.is_empty() && self.full_chunks.is_empty()
    }

    /// Collects the ack ids that need to be renewed when called. Will return at most `max_chunk_size` items to renew.
    ///
    /// The caller is expected to repeatedly call this function until it returns a chunk smaller than `max_chunk_size`,
    /// which means that we have collected all expired lease items.
    pub(crate) fn collect_ack_ids_that_need_to_be_renewed(
        &mut self,
        max_chunk_size: usize,
        max_lease_renewal_delay: Duration,
    ) -> Vec<(Duration, AckIdFromServer)> {
        // Since we never care about waking up any future when the next item in the delay queue expires, we'll simply
        // use a noop waker.
        let noop_waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&noop_waker);

        let now = tokio::time::Instant::now();
        let delay_range = (max_lease_renewal_delay / 2)..max_lease_renewal_delay;
        let mut rng = thread_rng();

        // Function that computes the next lease renewal instant, by spraying values between
        // (max_lease_renewal_duration / 2)..max_lease_renewal_duration.
        let mut next_lease_renewal_instant = || now + rng.gen_range(delay_range.clone());

        let mut result = Vec::new();
        while result.len() < max_chunk_size {
            let ack_id = match self.lease_renewal_delay_queue.poll_expired(&mut context) {
                Poll::Ready(Some(ack_id)) => ack_id.into_inner(),
                Poll::Ready(None) | Poll::Pending => break,
            };

            let ack_item = match self.ack_id_mapping.get_mut(&ack_id) {
                Some(ack_item) => ack_item,
                None => continue,
            };

            // Optimization: We have some expired items, so we'll go ahead and pre-reserve the entire chunk, to avoid
            // re-allocations of the vector.
            if result.capacity() == 0 {
                result.reserve(max_chunk_size);
            }

            // Re-schedule the lease renewal to the next deadline.
            ack_item.lease_delay_queue_key = self
                .lease_renewal_delay_queue
                .insert_at(ack_id, next_lease_renewal_instant());

            result.push((
                now.saturating_duration_since(ack_item.enqueued_at.into()),
                ack_item.ack_id_from_server.clone(),
            ));
        }

        result
    }

    /// Given an ack_id from the protobuf, exchange it for an [`AckId`], and store that we're currently holding onto
    /// a message with the provided `ack_id_from_server`.
    ///
    /// We will relinquish our hold of the `ack_id_from_server` once the returned [`AckId`] is provided to
    /// [`MessageAckQueue::enqueue_ack_or_nack`], and a batch containing the given AckId is collected by
    /// [`MessageAckQueue::next_chunk`].
    pub(crate) fn transform_and_store_ack_id(
        &mut self,
        ack_id_from_server: AckIdFromServer,
        received_at: Instant,
        lease_renew_at: tokio::time::Instant,
    ) -> AckId {
        let next_ack_id = self.last_ack_id.next();
        self.last_ack_id = next_ack_id;

        let lease_delay_queue_key = self
            .lease_renewal_delay_queue
            .insert_at(next_ack_id, lease_renew_at);

        self.ack_id_mapping.insert(
            next_ack_id,
            MessageAckQueueItem {
                ack_id_from_server,
                lease_delay_queue_key,
                enqueued_at: received_at,
            },
        );

        next_ack_id
    }

    /// Enqueues an ack to the batch that will eventually be consumed by `take_batch`. This removes the
    /// message from lease management.
    pub(crate) fn enqueue_ack_or_nack(&mut self, ack_or_nack: AckOrNack<AckId>) {
        if let Some(ack_or_nack) = self.remove_ack_or_nack(ack_or_nack) {
            // When the first message is placed into the ack queue, we'll mark the last flush as being
            // the time in which the message was added to the queue, meaning it *should* linger in the queue
            // for at most `pending_ack_flush_interval`.
            if self.is_empty() {
                self.reset_next_flush();
            }

            self.current_chunk.push(ack_or_nack);

            // If we have filled our chunk, we'll go ahead and move that chunk into the full chunk list,
            // and allocate a new empty chunk.
            if self.current_chunk.len() == self.max_chunk_size {
                let chunk = self.take_current_chunk();
                self.full_chunks.push_back(chunk);
            }
        }
    }

    /// Takes the current chunk, and replaces it with an empty chunk.
    fn take_current_chunk(&mut self) -> Vec<AckOrNack<AckIdFromServer>> {
        std::mem::replace(
            &mut self.current_chunk,
            Vec::with_capacity(self.max_chunk_size),
        )
    }

    /// Tries to take the next chunk of enqueued acks.
    ///
    /// This function has the following behavior:
    ///   - If there are no pending acks, this future will *never* resolve.
    ///   - If the number of pending acks is greater than or equal to `max_chunk_size`, this future will
    ///     resolve immediately with a Vec no larger than `max_chunk_size`, if there is additional remaining
    ///     we will not reset the last flush timestamp, such that the next call to this function will either resolve
    ///     instantly, or at the next flush interval.
    ///   - Otherwise, this future will resolve after after at least `pending_ack_flush_interval` with a non-empty
    ///     batch.
    ///
    /// NOTE: This future is cancel-safe. Cancelling this future before it is ready will not result in any lost chunks.
    pub(crate) async fn next_chunk(&mut self) -> Vec<AckOrNack<AckIdFromServer>> {
        if self.is_empty() {
            return pending().await;
        }

        match self.full_chunks.pop_front() {
            // We have a full chunk ready, so we can immediately dispense of the chunk.
            Some(pending_ack_chunk) => pending_ack_chunk,
            // We have no full chunk, so instead, we'll wait until `next_flush` before trying to dispense of the
            // non-empty current chunk.
            None => {
                tokio::time::sleep_until(self.next_flush.into()).await;
                self.take_current_chunk()
            }
        }
    }

    /// Removes the [`AckId`] from an [`AckOrNack`] using `remove_ack_id`, and returns the ack_kd_from_server
    /// as provided by [`MessageAckQueue::transform_and_store_ack_id`].
    fn remove_ack_or_nack(&mut self, ack_or_nack: AckOrNack) -> Option<AckOrNack<AckIdFromServer>> {
        self.remove_ack_id(ack_or_nack.ack_id())
            .map(|ack_id_from_server| ack_or_nack.map(|_| ack_id_from_server))
    }

    /// Removes an ack id from the `ack_id_mapping`, returning back the `ack_id_from_server` as provided by
    /// [`MessageAckQueue::transform_and_store_ack_id`].
    pub(crate) fn remove_ack_id(&mut self, ack_id: &AckId) -> Option<AckIdFromServer> {
        let MessageAckQueueItem {
            ack_id_from_server,
            lease_delay_queue_key,
            enqueued_at: _,
        } = self.ack_id_mapping.remove(ack_id)?;

        self.lease_renewal_delay_queue
            .remove(&lease_delay_queue_key);

        Some(ack_id_from_server)
    }

    /// Resets the next flush deadline by bumping it into the future by `pending_ack_flush_interval`.
    fn reset_next_flush(&mut self) {
        self.next_flush = Instant::now() + self.pending_ack_flush_interval;
    }
}
