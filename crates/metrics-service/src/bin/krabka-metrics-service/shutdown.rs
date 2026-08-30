/// A single process-wide shutdown signal shared by the HTTP server and every
/// background task.
///
/// A `true` value in the watch asks the axum server to start its graceful
/// drain, and tells the consumer and eval loops to stop. A critical background
/// task that exits also sets the watch, whether it exits cleanly or with an
/// error. The whole process then stops, instead of a continued run with a dead
/// loop.
#[derive(Clone)]
pub(crate) struct Shutdown {
    pub(crate) tx: tokio::sync::watch::Sender<bool>,
    pub(crate) rx: tokio::sync::watch::Receiver<bool>,
}

impl Shutdown {
    pub(crate) fn new() -> Self {
        let (tx, rx) = tokio::sync::watch::channel(false);
        Self { tx, rx }
    }

    /// Request shutdown.
    ///
    /// This method is idempotent. Repeated triggers do nothing.
    pub(crate) fn trigger(&self) {
        let _ = self.tx.send(true);
    }

    /// Return a future that resolves after a caller requests shutdown.
    ///
    /// Each consumer gets its own clone: the server's graceful-shutdown hook
    /// and each background task.
    pub(crate) fn signalled(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut rx = self.rx.clone();
        async move {
            // `borrow()` covers the already-triggered case; otherwise wait for the
            // next change. `changed()` only errors once every sender is dropped, by
            // which point we also want to stop, so treat that as "shut down".
            while !*rx.borrow_and_update() {
                if rx.changed().await.is_err() {
                    break;
                }
            }
        }
    }
}
