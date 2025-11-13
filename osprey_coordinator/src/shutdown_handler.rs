use crate::priority_queue::{PriorityQueueReceiver, PriorityQueueSender};
use crate::signals;

pub fn spawn_shutdown_handler(
    priority_queue_sender: PriorityQueueSender,
    priority_queue_receiver: PriorityQueueReceiver,
) {
    tokio::spawn(async move {
        tracing::info!("shutdown handler spawned - waiting on exit signal");
        signals::exit_signal().await;
        tracing::info!("got exit signal");
        priority_queue_receiver.nack_all_async();
        tracing::info!("nacked all outstanding async actions");
        tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
        priority_queue_sender.close();
        tracing::info!("closed priority queue");
    });
}
