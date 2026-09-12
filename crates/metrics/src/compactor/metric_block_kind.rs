use super::{Deserialize, Serialize};

/// Metric block payload kind used in deterministic object keys.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum MetricBlockKind {
    Float,
    NativeHistograms,
    Exemplars,
    Metadata,
    ClockReadings,
}

impl MetricBlockKind {
    pub(crate) const fn object_path(self) -> &'static str {
        match self {
            Self::Float => "float",
            Self::NativeHistograms => "native-histograms",
            Self::Exemplars => "exemplars",
            Self::Metadata => "metadata",
            Self::ClockReadings => "clock-readings",
        }
    }

    /// Whether a level compaction may merge two blocks of this kind into one.
    ///
    /// A merge reads its inputs in the declared `(fingerprint, timestamp)`
    /// order, drops the rows that repeat a key, and writes the rest as one
    /// block. Two kinds answer to that treatment and three do not.
    ///
    /// `Float` and `NativeHistograms` do. Both are keyed by
    /// `(fingerprint, timestamp)`, both carry a fixed schema, and a duplicate
    /// key is a duplicate sample: the `PromQL` engine already drops one of a
    /// pair at query time, so dropping it here changes no answer.
    ///
    /// The three exclusions each have their own reason:
    ///
    /// - `Exemplars`. `(fingerprint, timestamp)` is not a key. Several
    ///   exemplars legitimately share one fingerprint and one timestamp with
    ///   different trace ids, so a merge that deduplicated on that pair would
    ///   delete exemplars. Nothing on the read path deduplicates exemplars
    ///   either, so a merge that kept the duplicates would make them visible
    ///   to a user instead.
    /// - `Metadata`. The rows carry no real timestamp. `encode_metadata_rows`
    ///   writes a constant zero, so the sort key is degenerate. A metadata
    ///   row's identity is
    ///   `(metric_family_name, metric_type, help, unit)`: the read path
    ///   collects on exactly that four-tuple and discards the fingerprint, and
    ///   two rows of one family differ legitimately when the help text or the
    ///   unit changed. The read path already collapses the duplicates, so a
    ///   merge buys almost nothing.
    /// - `ClockReadings`. Nothing reads it on any query path.
    ///   `MetricBlockStore::from_compaction_manifests` registers no store for
    ///   it, because `PromQL` reads the float projection the distributor writes
    ///   beside it. It also carries five `Dictionary(Int32, Utf8)` columns, and
    ///   whether two blocks with disjoint `node` dictionaries concatenate is
    ///   unverified. Prove that first if this kind is ever to be merged.
    pub(crate) const fn is_mergeable(self) -> bool {
        match self {
            Self::Float | Self::NativeHistograms => true,
            Self::Exemplars | Self::Metadata | Self::ClockReadings => false,
        }
    }
}
