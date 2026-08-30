use super::{AT_MODIFIER_BOUNDS, AtModifierBounds};

/// Returns the range bounds in scope for `@ start()` and `@ end()` resolution.
///
/// Returns `None` outside a range query, that is, in an instant query.
pub(crate) fn current_at_modifier_bounds() -> Option<AtModifierBounds> {
    AT_MODIFIER_BOUNDS.try_with(|bounds| *bounds).ok()
}
