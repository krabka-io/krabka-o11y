use super::*;

/// The kernel clock discipline measurements from `adjtimex(2)`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TimexReading {
    /// The kernel `maxerror`, which the kernel grows at 500 ppm between
    /// updates, so it is already an uncertainty bound.
    pub max_error_nanos: i64,
    /// The kernel `esterror`, which is the discipline's own error estimate.
    pub est_error_nanos: i64,
    /// The kernel `STA_UNSYNC` bit.
    pub unsynchronized: bool,
}
