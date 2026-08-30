use super::NativeQuantileBucket;

pub(crate) fn bucket_overlap_fraction(bucket: NativeQuantileBucket, lower: f64, upper: f64) -> f64 {
    let overlap_lower = bucket.lower.max(lower);
    let overlap_upper = bucket.upper.min(upper);
    // `>=` against `>` is a permanent mutation survivor: the two differ only on
    // a zero-width overlap, and there the fall-through divides that zero width
    // by the bucket's own and returns the same 0.0.
    if overlap_lower >= overlap_upper {
        return 0.0;
    }
    if bucket.lower.is_infinite() || bucket.upper.is_infinite() {
        // An open-ended bucket counts in full, or not at all: a finite query
        // bound never reaches into one, however far out it goes.
        //
        // Only two open bounds get this far. A `+Inf` lower or a `-Inf` upper
        // leaves the overlap empty against any range -- `max(+Inf, lower)` is
        // never below `upper`, and `min(-Inf, upper)` is never above `lower` --
        // so the test above has already returned for them.
        if bucket.lower.is_infinite() {
            return f64::from(lower.is_infinite() && lower.is_sign_negative());
        }
        return f64::from(upper.is_infinite() && upper.is_sign_positive());
    }
    (overlap_upper - overlap_lower) / (bucket.upper - bucket.lower)
}
