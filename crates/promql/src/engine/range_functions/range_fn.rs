use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RangeFn {
    Rate,
    Increase,
    Delta,
    Changes,
    Resets,
}
