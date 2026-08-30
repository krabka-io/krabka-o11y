use super::*;

/// Clock reading reference-identity column (`Dictionary<Int32, Utf8>`).
///
/// The value holds the PTP `gmIdentity`, the NTP peer, or the GNSS
/// constellation.
pub const CCOL_REFERENCE_ID: &str = "reference_id";
