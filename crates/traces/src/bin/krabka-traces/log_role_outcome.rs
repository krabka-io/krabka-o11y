use krabka_observability::RoleKind;

/// Records why a role of `--target all` stopped.
///
/// Every role in the composition is boxed down to a future that returns
/// nothing, so this is the last place its error exists. The process still
/// notices the stop -- `StagedDrain` reports the stage by name and the exit
/// code follows from that -- but without this line the log would say a role is
/// gone and never say what it said on the way out.
pub(crate) fn log_role_outcome(
    role: RoleKind,
    outcome: Result<(), Box<dyn std::error::Error + Send + Sync>>,
) {
    match outcome {
        Ok(()) => tracing::info!(role = role.as_str(), "traces role stopped"),
        Err(error) => tracing::error!(role = role.as_str(), %error, "traces role stopped"),
    }
}
