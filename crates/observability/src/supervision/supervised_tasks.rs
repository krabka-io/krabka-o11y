use super::{CancellationToken, JoinHandle};

/// The background tasks a role cannot serve correct answers without, held by
/// the role for as long as it serves.
///
/// Register every long-lived task here instead of dropping its handle. The
/// role then selects its main future against
/// [`first_unexpected_exit`](SupervisedTasks::first_unexpected_exit): whichever
/// finishes first decides how the role ends, and a task that panics counts as
/// finishing.
///
/// The token is the role's shutdown token. Cancelling it is how a supervised
/// exit propagates: the remaining tasks and the listener see the cancel and
/// wind down, and exits observed after that point are the shutdown working
/// rather than a fault.
pub struct SupervisedTasks {
    token: CancellationToken,
    tasks: tokio::task::JoinSet<&'static str>,
}

impl SupervisedTasks {
    /// An empty supervisor over `token`.
    #[must_use]
    pub fn new(token: CancellationToken) -> Self {
        Self {
            token,
            tasks: tokio::task::JoinSet::new(),
        }
    }

    /// The role's shutdown token.
    #[must_use]
    pub fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// Whether nothing is being supervised yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Spawns `future` as a role-critical task called `name`.
    pub fn spawn<F>(&mut self, name: &'static str, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.adopt(name, tokio::spawn(future));
    }

    /// Takes over an already-spawned task, for a helper that spawns and hands
    /// its handle back.
    pub fn adopt(&mut self, name: &'static str, handle: JoinHandle<()>) {
        self.tasks.spawn(async move {
            match handle.await {
                Ok(()) => {}
                Err(error) if error.is_panic() => {
                    tracing::error!(task = name, %error, "background task panicked");
                }
                Err(error) => {
                    tracing::error!(task = name, %error, "background task was aborted");
                }
            }
            name
        });
    }

    /// Resolves with the name of the first supervised task to stop while the
    /// role was not shutting down, having cancelled the role's token.
    ///
    /// The future stays pending while every task runs, while no task is
    /// registered at all, and for exits observed after the token is cancelled
    /// -- those are the shutdown doing its work. That makes it safe to select
    /// a role's main future against unconditionally.
    ///
    /// A clean return counts as a fault too. These loops run for the life of
    /// the role, so returning `Ok(())` early means the loop decided to stop
    /// and told nobody, which reads the same from outside as a panic.
    pub async fn first_unexpected_exit(&mut self) -> &'static str {
        loop {
            let Some(joined) = self.tasks.join_next().await else {
                // Nothing left to watch. A role with no supervised tasks must
                // still be able to select on this, so never resolve.
                std::future::pending::<()>().await;
                continue;
            };
            let Ok(name) = joined else {
                // The wrapper above only ends by returning `name`; a join
                // error here means the set itself was aborted.
                continue;
            };
            if self.token.is_cancelled() {
                continue;
            }
            self.token.cancel();
            return name;
        }
    }

    /// Cancels the role's token and waits for every supervised task to stop.
    ///
    /// Call it on the way out so a graceful shutdown drains rather than
    /// detaching whatever was still running.
    pub async fn shutdown(mut self) {
        self.token.cancel();
        while self.tasks.join_next().await.is_some() {}
    }
}
