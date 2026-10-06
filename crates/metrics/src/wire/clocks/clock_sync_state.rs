use krabka_domain_macros::EnumName;

use super::{Deserialize, Serialize};

/// What a clock discipline does now.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize, EnumName)]
#[enum_name(accessor = "as_label")]
pub enum ClockSyncState {
    /// The clock tracks a valid reference.
    #[name(value = "synchronized")]
    Synchronized,
    /// The reference is gone. The clock runs on the rate it learned before.
    #[name(value = "holdover")]
    Holdover,
    /// The clock never had a reference.
    #[name(value = "free_running")]
    FreeRunning,
    /// The clock lost its reference and holds no rate estimate.
    #[name(value = "unsynchronized")]
    Unsynchronized,
    /// The discipline stepped the clock. Time is not continuous across this
    /// reading.
    #[name(value = "stepped")]
    Stepped,
}

impl ClockSyncState {
    /// Every discipline state, in wire order.
    ///
    /// The projection walks this list on every reading, so a state that stops
    /// being current gets an explicit zero rather than a stale one.
    pub const ALL: [Self; 5] = [
        Self::Synchronized,
        Self::Holdover,
        Self::FreeRunning,
        Self::Unsynchronized,
        Self::Stepped,
    ];
}
