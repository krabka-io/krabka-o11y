use super::*;

/// The fallback value a render-time parameter takes when absent, in Unix
/// milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct DefaultMs(pub i64);
