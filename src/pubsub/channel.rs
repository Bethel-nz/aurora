use super::events::ChangeEvent;
use tokio::sync::broadcast;

/// Wrapper around broadcast channel for change events
pub struct ChangeChannel {
    sender: broadcast::Sender<ChangeEvent>,
}

impl ChangeChannel {
    /// Create a new change channel with the specified buffer size
    pub fn new(buffer_size: usize) -> Self {
        let (sender, _) = broadcast::channel(buffer_size);
        Self { sender }
    }

    /// Publish an event to this channel
    pub fn publish(&self, event: ChangeEvent) -> Result<(), broadcast::error::SendError<ChangeEvent>> {
        self.sender.send(event).map(|_| ())
    }

    /// Subscribe to this channel
    pub fn subscribe(&self) -> broadcast::Receiver<ChangeEvent> {
        self.sender.subscribe()
    }

    /// Get the number of active receivers
    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Clone for ChangeChannel {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}
