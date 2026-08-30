#[cfg(feature = "experimental-functions")]
#[derive(Clone, Copy)]
pub(crate) struct QueryRangeContext {
    /// Range start, an epoch-millisecond instant.
    pub(crate) start_ms: i64,
    /// Range end, an epoch-millisecond instant.
    pub(crate) end_ms: i64,
    /// Grid resolution. This value is an extent, not an instant like the two
    /// bounds.
    pub(crate) step: Time,
}
