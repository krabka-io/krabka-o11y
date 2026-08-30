use super::{Deserialize, Serialize};

/// Where a clock gets its time.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum ClockSourceKind {
    /// IEEE 1588 Precision Time Protocol.
    Ptp,
    /// Network Time Protocol.
    Ntp,
    /// A satellite receiver.
    Gnss,
    /// The kernel clock discipline that `adjtimex(2)` reports.
    KernelTimex,
    /// A PTP hardware clock device.
    Phc,
}

impl ClockSourceKind {
    /// Every source kind, in wire order.
    pub const ALL: [Self; 5] = [
        Self::Ptp,
        Self::Ntp,
        Self::Gnss,
        Self::KernelTimex,
        Self::Phc,
    ];

    /// The label and dictionary value for this source kind.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Ptp => "ptp",
            Self::Ntp => "ntp",
            Self::Gnss => "gnss",
            Self::KernelTimex => "kernel_timex",
            Self::Phc => "phc",
        }
    }
}
