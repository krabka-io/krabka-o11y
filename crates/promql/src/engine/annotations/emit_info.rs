use super::ANNOTATIONS;

/// Records a `PromQL info:`-class annotation for the current query.
///
/// This function does nothing if no sink is in scope. See `emit_warning`.
pub(crate) fn emit_info(message: impl Into<String>) {
    let _ = ANNOTATIONS.try_with(|sink| sink.borrow_mut().info(message));
}
