use super::*;

#[derive(Clone, Copy)]
pub(crate) enum QueryKind {
    Instant,
    Range,
}
