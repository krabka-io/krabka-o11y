use super::*;

/// The GNSS receiver measurements.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GnssReading {
    /// The count of satellites in the position solution.
    pub satellites_used: u32,
    /// The quality of the position solution. A receiver that reports no fix
    /// quality leaves this empty.
    pub fix: Option<GnssFix>,
}
