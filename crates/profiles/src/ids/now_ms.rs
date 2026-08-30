use super::{Display, From, Into};

/// The "current" wall-clock instant a relative render time resolves against,
/// in Unix milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct NowMs(pub i64);
