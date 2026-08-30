use super::*;

/// Reunion spanSets across blocks.
///
/// This dedupes spans by `spanID` into the first spanSet, and accumulates each
/// spanSet's true `matched` count. The match count is additive across shards.
/// This is ported from the legacy `merge_span_sets`.
pub(crate) fn merge_span_sets(existing: &mut Vec<SpanSetJson>, incoming: Vec<SpanSetJson>) {
    for span_set in incoming {
        let Some(first) = existing.first_mut() else {
            existing.push(span_set);
            continue;
        };
        // `matched` is additive across shards, but only for *distinct* matches:
        // a span already present (a late-span / overlap duplicate) must not be
        // counted twice. Subtract the already-seen *returned* spans from this
        // set's reported `matched` before folding it in.
        //
        // Crucially we fold `matched` for EVERY set rather than skipping a set
        // whose returned spans all happen to be duplicates: under per-shard spss
        // truncation a set's returned spans are only a subset of what it matched,
        // so an overlapping returned subset does NOT make the set a pure
        // duplicate — its non-returned matches (`matched - duplicates`) are still
        // new and would otherwise be lost (an undercount).
        let duplicates = span_set
            .spans
            .iter()
            .filter(|s| first.spans.iter().any(|e| e.span_id == s.span_id))
            .count();
        let new_matches = span_set
            .matched
            .saturating_sub(u32::try_from(duplicates).unwrap_or(u32::MAX));
        first.matched = first.matched.saturating_add(new_matches);
        for span in span_set.spans {
            if !first.spans.iter().any(|s| s.span_id == span.span_id) {
                first.spans.push(span);
            }
        }
    }
}
