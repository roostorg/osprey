use crate::metrics::gauges::StaticGauge;
use crate::metrics::histograms::StaticHistogram;
use tokio::{
    sync::oneshot,
    time::{interval, Duration, Instant, MissedTickBehavior},
};

use crate::{coordinator_metrics::SmiteCoordinatorMetrics, proto};

use crate::tokio_utils::AbortOnDrop;
use std::{cell::Cell, sync::Arc};

#[derive(Debug)]
pub enum AckOrNack {
    Ack(Option<crate::proto::Verdicts>),
    Nack,
}

impl From<proto::ack_or_nack::AckOrNack> for AckOrNack {
    fn from(ack_or_nack: proto::ack_or_nack::AckOrNack) -> Self {
        match ack_or_nack {
            proto::ack_or_nack::AckOrNack::Ack(inner) => Self::Ack(inner.verdicts),
            proto::ack_or_nack::AckOrNack::Nack(_) => Self::Nack,
        }
    }
}

pub struct AckableAction {
    pub action: proto::SmiteCoordinatorAction,
    acking_oneshot_sender: oneshot::Sender<AckOrNack>,
    local_retry_count: Cell<u32>,
    pub created_at: Instant,
}

impl AckableAction {
    pub fn new(
        action: proto::SmiteCoordinatorAction,
    ) -> (
        AckableAction,
        oneshot::Receiver<crate::priority_queue::AckOrNack>,
    ) {
        let (acking_oneshot_sender, acking_oneshot_receiver) = oneshot::channel::<AckOrNack>();
        let ackable_action = AckableAction {
            action,
            acking_oneshot_sender,
            local_retry_count: 0.into(),
            created_at: Instant::now(),
        };
        (ackable_action, acking_oneshot_receiver)
    }

    pub fn into_action(self) -> (proto::SmiteCoordinatorAction, ActionAcker) {
        (
            self.action,
            ActionAcker {
                acking_oneshot_sender: self.acking_oneshot_sender,
            },
        )
    }

    fn increment_retry_count(&self) {
        let count = self.local_retry_count.get();
        self.local_retry_count.set(count + 1);
    }

    #[allow(unused)]
    pub fn retry_count(&self) -> u32 {
        self.local_retry_count.get()
    }
}

#[derive(Debug)]
pub struct ActionAcker {
    acking_oneshot_sender: oneshot::Sender<AckOrNack>,
}

impl ActionAcker {
    pub fn ack_or_nack<T: Into<AckOrNack>>(self, ack_or_nack: T) {
        self.acking_oneshot_sender.send(ack_or_nack.into()).ok();
    }
}

pub enum Priority {
    Sync,
    Async,
}

#[derive(Clone)]
pub struct PriorityQueueSender {
    sync_sender: async_channel::Sender<AckableAction>,
    async_sender: async_channel::Sender<AckableAction>,
}

impl PriorityQueueSender {
    fn new(
        sync_sender: async_channel::Sender<AckableAction>,
        async_sender: async_channel::Sender<AckableAction>,
    ) -> PriorityQueueSender {
        PriorityQueueSender {
            sync_sender,
            async_sender,
        }
    }

    pub fn close(&self) {
        self.sync_sender.close();
        self.async_sender.close();
    }
    pub async fn send_sync(
        &self,
        ackable_action: AckableAction,
    ) -> Result<(), async_channel::SendError<AckableAction>> {
        self.send(ackable_action, Priority::Sync).await
    }

    pub async fn send_async(
        &self,
        ackable_action: AckableAction,
    ) -> Result<(), async_channel::SendError<AckableAction>> {
        self.send(ackable_action, Priority::Async).await
    }

    async fn send(
        &self,
        ackable_action: AckableAction,
        priority: Priority,
    ) -> Result<(), async_channel::SendError<AckableAction>> {
        ackable_action.increment_retry_count();
        match priority {
            Priority::Sync => self.sync_sender.send(ackable_action).await,
            Priority::Async => self.async_sender.send(ackable_action).await,
        }
    }

    pub fn len_sync(&self) -> usize {
        self.sync_sender.len()
    }

    pub fn len_async(&self) -> usize {
        self.async_sender.len()
    }

    pub fn receiver_count_sync(&self) -> usize {
        self.sync_sender.receiver_count()
    }

    pub fn receiver_count_async(&self) -> usize {
        self.async_sender.receiver_count()
    }
}

#[derive(Clone)]
pub struct PriorityQueueReceiver {
    sync_receiver: async_channel::Receiver<AckableAction>,
    async_receiver: async_channel::Receiver<AckableAction>,
}

impl PriorityQueueReceiver {
    fn new(
        sync_receiver: async_channel::Receiver<AckableAction>,
        async_receiver: async_channel::Receiver<AckableAction>,
    ) -> PriorityQueueReceiver {
        PriorityQueueReceiver {
            sync_receiver,
            async_receiver,
        }
    }
    pub async fn recv(
        &self,
        metrics: Arc<SmiteCoordinatorMetrics>,
    ) -> Result<AckableAction, async_channel::RecvError> {
        loop {
            let result = tokio::select! {
                biased;
                result = self.sync_receiver.recv() => result,
                result = self.async_receiver.recv() => match result {
                    Ok(ackable_action) => {
                        metrics.action_time_in_async_queue.record(Instant::now().duration_since(ackable_action.created_at));
                        Ok(ackable_action)
                    }
                    Err(_) => self.sync_receiver.recv().await
                },
            };
            match result {
                Ok(ackable_action) => {
                    // If the acking oneshot receiver is closed then there is no reason to process this action
                    // This can happen if the client sending a sync classification request times out
                    if ackable_action.acking_oneshot_sender.is_closed() {
                        continue;
                    } else {
                        return Ok(ackable_action);
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn nack_all_async(&self) {
        loop {
            match self.async_receiver.try_recv() {
                Ok(action) => match action.acking_oneshot_sender.send(AckOrNack::Nack) {
                    Ok(_) => (),
                    Err(_) => println!(
                        "tried to nack {:?} and the nacking receiver was dropped",
                        action.action
                    ),
                },
                Err(_) => return,
            }
        }
    }
}

pub fn create_ackable_action_priority_queue() -> (PriorityQueueSender, PriorityQueueReceiver) {
    let (sync_sender, sync_receiver) = async_channel::unbounded();
    let (async_sender, async_receiver) = async_channel::unbounded();
    (
        PriorityQueueSender::new(sync_sender, async_sender),
        PriorityQueueReceiver::new(sync_receiver, async_receiver),
    )
}

pub fn spawn_priority_queue_metrics_worker(
    queue_sender: PriorityQueueSender,
    metrics: Arc<SmiteCoordinatorMetrics>,
) -> AbortOnDrop<()> {
    let mut interval = interval(Duration::from_millis(100));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let join_handle = tokio::task::spawn(async move {
        loop {
            interval.tick().await;
            metrics
                .priority_queue_size_sync
                .set(queue_sender.len_sync() as u64);
            metrics
                .priority_queue_size_async
                .set(queue_sender.len_async() as u64);
            metrics
                .priority_queue_receiver_count_async
                .set(queue_sender.receiver_count_async() as u64);
            metrics
                .priority_queue_receiver_count_sync
                .set(queue_sender.receiver_count_sync() as u64);
        }
    });

    AbortOnDrop::new(join_handle)
}
