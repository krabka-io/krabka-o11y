use super::{Display, From, Into};

/// The earliest span `start_ns` in a flushed block-builder window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct WindowStartNs(pub i64);
