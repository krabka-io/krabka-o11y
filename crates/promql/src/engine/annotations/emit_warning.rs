use super::ANNOTATIONS;

/// Records a `PromQL warning:`-class annotation for the current query.
///
/// This function does nothing if no sink is in scope. A unit test that calls
/// internals directly is outside a scoped query, so a call is always safe.
pub(crate) fn emit_warning(message: impl Into<String>) {
    let _ = ANNOTATIONS.try_with(|sink| sink.borrow_mut().warn(message));
}
