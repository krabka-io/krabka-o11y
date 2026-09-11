use krabka_observability::CancellationToken;

/// A single process-wide shutdown signal shared by the HTTP server and every
/// background task.
///
/// Triggering it asks the axum server to start its graceful drain and tells
/// the consumer and eval loops to stop. The role's supervisor triggers it too,
/// as soon as a critical background task stops for any reason, so the process
/// winds down instead of running on with a dead loop.
///
/// It is a [`CancellationToken`] underneath, which is what
/// [`SupervisedTasks`](krabka_observability::SupervisedTasks) supervises
/// against: one signal, not two that have to be kept in step.
#[derive(Clone)]
pub(crate) struct Shutdown {
    token: CancellationToken,
}

impl Shutdown {
    pub(crate) fn new() -> Self {
        Self {
            token: CancellationToken::new(),
        }
    }

    /// The token the role's supervisor cancels, and that its tasks watch.
    pub(crate) fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// Request shutdown.
    ///
    /// This method is idempotent. Repeated triggers do nothing.
    pub(crate) fn trigger(&self) {
        self.token.cancel();
    }

    /// Whether shutdown has been requested, for a loop's stop predicate.
    pub(crate) fn is_triggered(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Return a future that resolves after a caller requests shutdown.
    ///
    /// Each consumer gets its own clone: the server's graceful-shutdown hook
    /// and each background task. A token that is already cancelled resolves it
    /// on the first poll, so a task that starts after the trigger does not
    /// hang.
    pub(crate) fn signalled(&self) -> impl Future<Output = ()> + Send + 'static {
        let token = self.token.clone();
        async move { token.cancelled_owned().await }
    }
}
