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
    F: Fn(i64, i64) -> Fut,
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
        if range_only && guard.unions.is_none() {
            None
        } else {
            guard.fill_window(&key, start_ms, end_ms)
        }
    };
    let Some(mut fill) = fill else {
        return Ok(Arc::new(load(start_ms, end_ms).await?));
    };
    let loaded = load(fill.start_ms, fill.end_ms).await;
    let expanded = fill.start_ms != start_ms || fill.end_ms != end_ms;
    let value = if expanded
        && matches!(
            &loaded,
            Err(crate::PromqlError::Limit(
                krabka_metrics::LimitError::SamplesPerQueryExceeded { .. }
                    | krabka_metrics::LimitError::SeriesPerQueryExceeded { .. }
            ))
        ) {
        // A union can exceed a limit even when each requested window fits.
        // Disable this union and retain only the requested window.
        {
            let mut guard = cache.lock().expect("range scan cache poisoned");
            if let Some(unions) = &mut guard.unions {
                unions.insert(
                    key.clone(),
                    super::range_scan_cache_inner::TimeWindow { start_ms, end_ms },
                );
            }
            guard.rows.remove(&key);
            guard.labels.remove(&key);
        }
        fill = super::range_scan_cache_inner::TimeWindow { start_ms, end_ms };
        load(start_ms, end_ms).await?
    } else {
        loaded?
    };
    let value = Arc::new(value);
    let mut guard = cache.lock().expect("range scan cache poisoned");
    entries(&mut guard).insert(
        key,
        Windowed {
            start_ms: fill.start_ms,
            end_ms: fill.end_ms,
            value: Arc::clone(&value),
        },
    );
    Ok(value)
}
