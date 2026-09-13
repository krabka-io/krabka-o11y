use super::{ANNOTATIONS, with_source_position};

/// Records a `PromQL info:`-class annotation for the current query.
///
/// This function does nothing if no sink is in scope. See `emit_warning`.
pub(crate) fn emit_info(message: impl Into<String>) {
    let message = with_source_position(message.into());
    let _ = ANNOTATIONS.try_with(|sink| sink.borrow_mut().info(message));
}
