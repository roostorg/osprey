use std::future::Future;

use crate::tokio_utils::AbortOnDrop;

use crate::pub_sub_streaming_pull::{DetachedMessage, Message};

/// The message handler is invoked by the streaming pull manager when there are messages to process.
///
/// When the [`crate::StreamingPullManager`] is created, the message handler is moved into the streaming pull manager's
/// task. If the the message handler's `handle_messages` function panicks, this will cause the streaming pull manager to
/// panic as well. So, exercise caution to ensure this method does not panic (unless something really bad has happened).
pub trait MessageHandler: Send + Sync + 'static {
    /// Invoked when there are more messages to handle.
    ///
    /// The streaming pull manager will limit the number of in-flight messages (that is to say, messages that are
    /// outstanding that have not been acked/nacked) based upon the settings configured in [`crate::FlowControl`].
    ///
    /// Thus, you should not need to implement any kind of concurrency limiting in your message handler, simply
    /// configure the [`crate::FlowControl`] accordingly.
    fn handle_messages(&mut self, messages: Vec<Message>);

    /// Invoked when the streaming pull manager begins its graceful stop process. After this method is invoked,
    /// `handle_messages` will no longer be called.
    ///
    /// This method can return an optional AbortOnDrop, which will be awaited upon during the graceful stop procedure.
    ///
    /// Most implementers of `MessageHandler` will not need to implement this method, however, some may benefit
    /// from the knowledge that there will be no further messages handled.
    fn handle_graceful_stop(&mut self) -> Option<AbortOnDrop> {
        None
    }
}

/// This utility struct will process each message by spawning a task that invokes the provided `message_handler`,
/// which will return a future that will then be driven to completion by [`tokio::task::spawn`]. The future must return
/// a [`Result<(), ()>`]. The message will be acked if the result is [`Ok`], or nacked if the result is [`Err`].
pub struct SpawnTaskPerMessageHandler<F> {
    message_handler: F,
}

impl<F> SpawnTaskPerMessageHandler<F> {
    pub fn new(message_handler: F) -> Self {
        Self { message_handler }
    }
}

impl<F, R> MessageHandler for SpawnTaskPerMessageHandler<F>
where
    R: Future<Output = Result<(), ()>> + Send + 'static,
    F: Fn(DetachedMessage) -> R + Send + Sync + 'static,
{
    fn handle_messages(&mut self, messages: Vec<Message>) {
        for message in messages {
            let (message, guard) = message.into_inner();
            let future = (self.message_handler)(message);
            tokio::task::spawn(async move {
                match future.await {
                    Ok(()) => guard.ack(),
                    Err(()) => guard.nack(),
                }
            });
        }
    }
}
