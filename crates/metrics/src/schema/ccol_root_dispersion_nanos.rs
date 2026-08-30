/// NTP root dispersion column (`Int64`).
///
/// RFC 5905 names the sum of half the root delay and the root dispersion the
/// synchronization distance. That sum is the real NTP uncertainty bound, and
/// neither term alone is.
pub const CCOL_ROOT_DISPERSION_NANOS: &str = "root_dispersion_nanos";
