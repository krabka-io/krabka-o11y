use super::{Arc, AtomicBool, AtomicOrdering, CancellationToken, JoinHandle, Time, TimeExt};

struct Stage {
    name: &'static str,
    token: CancellationToken,
    task: JoinHandle<()>,
}

/// The roles an all-in-one process runs, and the order it stops them in.
///
/// Roles that share a process share a stop. A `--target all` that cancelled
/// one token for all of them would stop the distributor and the block builder
/// at the same instant, and the records the distributor had already written to
/// the WAL but the block builder had not yet read would be left for whatever
/// restarted next -- which, on a laptop or a one-replica deployment, is
/// nothing. The stop has an order, and the order is the whole point:
///
/// 1. the distributor stops accepting writes, so nothing new enters the WAL;
/// 2. the block builder drains what is already in the WAL and commits it;
/// 3. the read path and the background jobs stop, having nothing to lose.
///
/// Each stage gets a [`CancellationToken`] of its own. [`drain`](Self::drain)
/// cancels them one at a time, in registration order, and waits for that
/// stage's task to finish before it asks the next stage to stop. A stage that
/// does not finish inside `stage_timeout` is left behind rather than allowed
/// to hold the whole stop open past an orchestrator's grace period; `drain`
/// names it so the process can say which stage it gave up on.
///
/// While the roles run, this is also their supervisor.
/// [`first_unexpected_exit`](Self::first_unexpected_exit) reports a stage that
/// stopped on its own -- returned, errored or panicked -- which in a
/// single-process stack means one role of the signal is gone while the port
/// stays open. Unlike [`SupervisedTasks`](super::SupervisedTasks) it cancels
/// nothing when that happens: the drain still has an order to keep, and the
/// caller runs it.
pub struct StagedDrain {
    stages: Vec<Stage>,
    exits: tokio::sync::mpsc::UnboundedSender<&'static str>,
    exited: tokio::sync::mpsc::UnboundedReceiver<&'static str>,
    draining: Arc<AtomicBool>,
    stage_timeout: Time,
}

impl StagedDrain {
    /// An empty drain whose stages each get `stage_timeout` to finish.
    #[must_use]
    pub fn new(stage_timeout: Time) -> Self {
        let (exits, exited) = tokio::sync::mpsc::unbounded_channel();
        Self {
            stages: Vec::new(),
            exits,
            exited,
            draining: Arc::new(AtomicBool::new(false)),
            stage_timeout,
        }
    }

    /// Adds a stage and starts it, handing `role` the token that stops it.
    ///
    /// Stages stop in the order they are added, so add the distributor before
    /// the block builder that drains behind it.
    ///
    /// `role` receives the stage's own token and must return once it is
    /// cancelled. A role that ignores its token is not stopped by this; it is
    /// waited for, and then reported by [`drain`](Self::drain) as a stage that
    /// ran out of time.
    pub fn stage<F, Fut>(&mut self, name: &'static str, role: F)
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let token = CancellationToken::new();
        // Two tasks, not one. The role runs in the inner task and the outer
        // one joins it, so a role that unwinds is still reported: a panic
        // inside the role would otherwise take the notification with it, and
        // the process would be one role short with nothing resolving and
        // nobody saying so -- which is the failure this type exists to catch.
        let role_task = tokio::spawn(role(token.clone()));
        let exits = self.exits.clone();
        let task = tokio::spawn(async move {
            match role_task.await {
                Ok(()) => {}
                Err(error) if error.is_panic() => {
                    tracing::error!(stage = name, %error, "role panicked");
                }
                Err(error) => {
                    tracing::error!(stage = name, %error, "role was aborted");
                }
            }
            let _ = exits.send(name);
        });
        self.stages.push(Stage { name, token, task });
    }

    /// The stages, in the order [`drain`](Self::drain) stops them.
    #[must_use]
    pub fn order(&self) -> Vec<&'static str> {
        self.stages.iter().map(|stage| stage.name).collect()
    }

    /// Resolves with the name of the first stage to stop while no drain was in
    /// progress.
    ///
    /// Stays pending while every stage runs, while no stage is registered, and
    /// for every exit observed once [`drain`](Self::drain) has begun -- those
    /// are the stop doing its work. That makes it safe to select a process's
    /// main future against unconditionally.
    pub async fn first_unexpected_exit(&mut self) -> &'static str {
        loop {
            let Some(name) = self.exited.recv().await else {
                // Every sender is gone, which cannot happen while `self` holds
                // one. Nothing further will ever arrive, so never resolve.
                std::future::pending::<()>().await;
                continue;
            };
            if self.draining.load(AtomicOrdering::SeqCst) {
                continue;
            }
            return name;
        }
    }

    /// Stops every stage in order, and returns the stages that did not finish
    /// in time.
    ///
    /// Each stage is cancelled only once the stage before it has finished, so
    /// a block builder registered after a distributor gets a WAL that nothing
    /// is still writing to.
    pub async fn drain(self) -> Vec<&'static str> {
        self.draining.store(true, AtomicOrdering::SeqCst);
        let timeout = self.stage_timeout.to_std();
        let mut overran = Vec::new();
        for stage in self.stages {
            stage.token.cancel();
            // The joined task is the wrapper from `stage`, which reports how
            // the role ended and then returns; it cannot itself fail, so a
            // timeout is the only outcome worth acting on here.
            if tokio::time::timeout(timeout, stage.task).await.is_err() {
                tracing::warn!(
                    stage = stage.name,
                    timeout = %timeout.as_secs_f64(),
                    "role did not finish draining in time; stopping the next one anyway"
                );
                overran.push(stage.name);
            }
        }
        overran
    }
}
