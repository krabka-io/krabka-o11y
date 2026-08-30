/// The instant functions evaluated over only the last two samples of the
/// window: `irate` and `idelta`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum InstantKind {
    /// Per-second instant rate from the last two samples, reset-clamped.
    Irate,
    /// Difference of the last two samples. This is a gauge function with no
    /// per-second division.
    Idelta,
}
