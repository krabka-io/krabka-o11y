use super::*;

pub(crate) fn is_duration_field(field: &Field) -> bool {
    matches!(
        field.scope,
        Scope::Intrinsic(
            Intrinsic::Duration | Intrinsic::TraceDuration | Intrinsic::EventTimeSinceStart
        )
    )
}
