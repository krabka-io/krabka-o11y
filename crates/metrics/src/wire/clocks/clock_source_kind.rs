use krabka_domain_macros::EnumName;

use super::{Deserialize, Serialize};

/// Where a clock gets its time.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize, EnumName)]
#[enum_name(accessor = "as_label")]
pub enum ClockSourceKind {
    /// IEEE 1588 Precision Time Protocol.
    #[name(value = "ptp")]
    Ptp,
    /// Network Time Protocol.
    #[name(value = "ntp")]
    Ntp,
    /// A satellite receiver.
    #[name(value = "gnss")]
    Gnss,
    /// The kernel clock discipline that `adjtimex(2)` reports.
    #[name(value = "kernel_timex")]
    KernelTimex,
    /// A PTP hardware clock device.
    #[name(value = "phc")]
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
}
