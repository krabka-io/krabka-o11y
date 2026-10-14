use super::{BTreeMap, CompareGroup};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CompareCountKey {
    pub(crate) group: CompareGroup,
    pub(crate) attr_key: String,
    pub(crate) value: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CompareAttributeKey {
    pub(crate) group: CompareGroup,
    pub(crate) attr_key: String,
}

/// Upper bound on the distinct values tracked per `(group, attribute)`.
///
/// The bound applies during accumulation, before the `top_n` cut. Without the
/// bound, a high-cardinality attribute such as a URL, a request id, or a UUID
/// on every span inserts one bucket-length `Vec` per distinct value and
/// exhausts memory. When a `(group, attribute)` reaches this cap, the code
/// drops new distinct values, and the values it already tracks keep counting.
/// Memory then stays at `O(attrs * cap * buckets)`. The code clamps the cap to
/// at least `top_n`, so the final `top_n` cut in `build_compare_series` is
/// never starved.
pub(crate) type CompareCounts = BTreeMap<CompareCountKey, Vec<u64>>;
