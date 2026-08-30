use super::*;

pub(crate) fn hinted_max_exemplars(default: usize, hint: Option<bool>) -> usize {
    match hint {
        Some(false) => 0,
        Some(true) | None => default,
    }
}
