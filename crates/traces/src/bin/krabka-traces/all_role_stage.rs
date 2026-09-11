use super::CancellationToken;

/// One role of `--target all`, not yet started, waiting for the token that
/// will stop it.
///
/// [`StagedDrain::stage`] takes the role as a closure over its own
/// [`CancellationToken`], and the seven roles have seven different return
/// types and seven different argument lists. Boxing them behind one signature
/// is what lets the composition be a list that
/// [`DRAIN_ORDER`](krabka_traces::all_in_one::DRAIN_ORDER) can be walked
/// against, rather than seven `drain.stage(...)` calls whose order is whatever
/// order someone happened to write them in.
///
/// The output is `()`, not a `Result`: a role that fails has already logged
/// why, and what the process does about it does not depend on which one it
/// was. `StagedDrain` reports the stage's *name* through
/// [`first_unexpected_exit`](krabka_observability::StagedDrain::first_unexpected_exit),
/// and that is what turns into the process's exit code.
///
/// [`StagedDrain::stage`]: krabka_observability::StagedDrain::stage
pub(crate) type AllRoleStage =
    Box<dyn FnOnce(CancellationToken) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

/// Wraps one role's body as an [`AllRoleStage`].
pub(crate) fn all_role_stage<F, Fut>(role: F) -> AllRoleStage
where
    F: FnOnce(CancellationToken) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Box::new(move |token| Box::pin(role(token)))
}
