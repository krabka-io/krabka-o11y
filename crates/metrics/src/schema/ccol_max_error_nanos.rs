/// Kernel timex `maxerror` column (`Int64`).
///
/// `adjtimex(2)` grows `maxerror` at 500 ppm between updates and sets the
/// `STA_UNSYNC` bit at 16 s, so this column is already an uncertainty bound.
pub const CCOL_MAX_ERROR_NANOS: &str = "max_error_nanos";
