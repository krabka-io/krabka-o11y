use super::Error;

/// The failure a role reports when one of its supervised background tasks
/// stopped while the role was still meant to be serving.
///
/// The name is the one the role registered with
/// [`SupervisedTasks`](super::SupervisedTasks), so the log line and the exit
/// both say which task went, rather than that something did.
#[derive(Debug, Error)]
#[error("critical background task `{0}` stopped unexpectedly")]
pub struct CriticalTaskError(pub &'static str);
