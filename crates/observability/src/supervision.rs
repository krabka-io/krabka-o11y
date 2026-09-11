//! Keeping the death of a background task visible.
//!
//! A role's long-lived tasks are the role. The WAL consumer is what makes a
//! querier's recent window recent, the index refresher is what makes newly
//! compacted blocks visible, and the accept loop is what makes the port a
//! port. A `tokio::spawn` whose `JoinHandle` is dropped detaches
//! all of that: the future is polled until it returns or until it
//! unwinds, and afterwards nobody is left to tell which happened.
//!
//! A task body that cancels a shutdown token in its `Err` arm covers the
//! first case only. A panic never reaches the `if let`. The task unwinds, the
//! handle is already gone, no `JoinError` is observed, the token stays
//! uncancelled, and the process keeps answering HTTP from a tier that stopped
//! advancing. That is worse than a crash: a restart is loud and bounded,
//! whereas a querier serving from a frozen frontier returns wrong answers and
//! says nothing.
//!
//! [`SupervisedTasks`] is where a role keeps those handles. It names each
//! task, joins them all, and resolves as soon as one stops while the role was
//! not shutting down -- whether it returned, errored, or panicked. The role
//! then cancels its token and returns [`CriticalTaskError`], so the process
//! exits non-zero and an orchestrator restarts it.
//!
//! Not every spawn belongs here. A shutdown-signal listener is *supposed* to
//! finish, and a per-request or per-connection task dying is one request's
//! problem, not the role's. Supervise what the role cannot serve correct
//! answers without.

use crate::{Arc, AtomicBool, AtomicOrdering, CancellationToken, Error, JoinHandle, Time, TimeExt};

mod critical_task_error;
mod staged_drain;
mod supervised_tasks;
#[cfg(test)]
mod tests;

pub use critical_task_error::CriticalTaskError;
pub use staged_drain::StagedDrain;
pub use supervised_tasks::SupervisedTasks;
