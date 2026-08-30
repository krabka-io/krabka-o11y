use super::{Deserialize, Serialize};

/// What a clock discipline does now.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum ClockSyncState {
    /// The clock tracks a valid reference.
    Synchronized,
    /// The reference is gone. The clock runs on the rate it learned before.
    Holdover,
    /// The clock never had a reference.
    FreeRunning,
    /// The clock lost its reference and holds no rate estimate.
    Unsynchronized,
    /// The discipline stepped the clock. Time is not continuous across this
    /// reading.
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

    /// The label and dictionary value for this state.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Synchronized => "synchronized",
            Self::Holdover => "holdover",
            Self::FreeRunning => "free_running",
            Self::Unsynchronized => "unsynchronized",
            Self::Stepped => "stepped",
        }
    }
}
