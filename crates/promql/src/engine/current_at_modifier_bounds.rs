use super::{AT_MODIFIER_BOUNDS, AtModifierBounds};

/// Returns the active query bounds for `@ start()` and `@ end()` resolution.
pub(crate) fn current_at_modifier_bounds() -> Option<AtModifierBounds> {
    AT_MODIFIER_BOUNDS.try_with(|bounds| *bounds).ok()
}
