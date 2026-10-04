use super::{
    Arc, HashMap, LabelMatcher, RANGE_SCAN_CACHE, RangeScanCacheInner, Result, Windowed,
    matchers_cache_key,
};

/// Answers a request for `[start_ms, end_ms]` through the scan cache of the
/// current query, and calls `load` only when the cache cannot answer.
///
/// `entries` picks the map of the cache that holds this kind of value. With
/// `range_only`, an instant-query scope does not keep the value. The
/// cache keeps one value per matcher set, and a new kept value replaces the
/// old one, so the memory that a query holds is one scan per matcher set. A
/// kept value can cover more than `[start_ms, end_ms]`, so every caller narrows
/// it before use. Outside a cache scope, and for a request that the cache does
/// not keep, this calls `load` for the requested window.
pub(crate) async fn through_scan_cache<T, F, Fut>(
    entries: fn(&mut RangeScanCacheInner) -> &mut HashMap<String, Windowed<T>>,
    range_only: bool,
    matchers: &[LabelMatcher],
    start_ms: i64,
    end_ms: i64,
    load: F,
) -> Result<Arc<T>>
where
    F: FnOnce(i64, i64) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let Ok(cache) = RANGE_SCAN_CACHE.try_with(Arc::clone) else {
        return Ok(Arc::new(load(start_ms, end_ms).await?));
    };
    let key = matchers_cache_key(matchers);
    let fill = {
        let mut guard = cache.lock().expect("range scan cache poisoned");
        if let Some(value) = entries(&mut guard)
            .get(&key)
            .and_then(|kept| kept.covering(start_ms, end_ms))
        {
            return Ok(value);
        }
        if range_only && guard.union.is_none() {
            None
        } else {
            guard.fill_window(start_ms, end_ms)
        }
    };
    let Some((fill_start_ms, fill_end_ms)) = fill else {
        return Ok(Arc::new(load(start_ms, end_ms).await?));
    };
    let value = Arc::new(load(fill_start_ms, fill_end_ms).await?);
    let mut guard = cache.lock().expect("range scan cache poisoned");
    entries(&mut guard).insert(
        key,
        Windowed {
            start_ms: fill_start_ms,
            end_ms: fill_end_ms,
            value: Arc::clone(&value),
        },
    );
    Ok(value)
}
