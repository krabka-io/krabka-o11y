use super::*;

/// The NTP measurements from RFC 5905.
///
/// RFC 5905 names the sum of half the root delay and the root dispersion the
/// synchronization distance. That sum is the real NTP uncertainty bound, and
/// neither term alone is, so both terms travel together.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NtpReading {
    /// The round-trip delay to the stratum 1 root.
    pub root_delay_nanos: i64,
    /// The accumulated dispersion to the stratum 1 root.
    pub root_dispersion_nanos: i64,
    /// The distance in NTP hops from a reference clock.
    pub stratum: u32,
}
