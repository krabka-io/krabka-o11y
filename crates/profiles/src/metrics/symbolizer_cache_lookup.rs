/// Result of one lookup in the per-pass uploaded-symbol cache, as the
/// `symbolizer_cache_requests{status}` counter records it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SymbolizerCacheLookup {
    /// The build id was in the cache: `status="hit"`.
    Hit,
    /// The build id was not in the cache: `status="miss"`.
    Miss,
}

impl SymbolizerCacheLookup {
    /// The `status` label value for this lookup.
    #[must_use]
    pub const fn status(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
        }
    }
}
