use super::Arc;

/// One cached value and the inclusive window of the store request that
/// produced it.
pub(crate) struct Windowed<T> {
    pub(crate) start_ms: i64,
    pub(crate) end_ms: i64,
    pub(crate) value: Arc<T>,
}

impl<T> Windowed<T> {
    /// The value, when its window holds all of `[start_ms, end_ms]`.
    pub(crate) fn covering(&self, start_ms: i64, end_ms: i64) -> Option<Arc<T>> {
        (start_ms >= self.start_ms && end_ms <= self.end_ms).then(|| Arc::clone(&self.value))
    }
}
