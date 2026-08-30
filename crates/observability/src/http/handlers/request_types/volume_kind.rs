use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VolumeKind {
    Instant,
    Range,
}
