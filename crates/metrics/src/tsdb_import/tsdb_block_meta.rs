use super::BTreeMap;

/// The parts of a block's `meta.json` that the import checks.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TsdbBlockMeta {
    pub min_time: i64,
    /// The exclusive upper bound of the block's samples.
    pub max_time: i64,
    /// The `thanos.labels` external labels, if the block carries any.
    pub external_labels: BTreeMap<String, String>,
}
