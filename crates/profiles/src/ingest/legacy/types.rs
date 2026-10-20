use std::collections::HashMap;

use super::{ByteSize, Deserialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestFormat {
    Pprof,
    Jfr,
    Trie,
    Tree,
    Lines,
    Speedscope,
    Groups,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestQuery {
    pub name: String,
    pub profile_type_suffix: Option<String>,
    pub labels: Vec<krabka_blockstore::LabelPair>,
    pub format: IngestFormat,
    pub sample_rate: u32,
    pub units: String,
    pub from_ms: Option<i64>,
    pub until_ms: Option<i64>,
    /// The profiler that produced the upload. Defaults to `unknown`.
    pub spy_name: String,
    /// Async-profiler event mode reported by the JFR producer.
    pub jfr_event: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct JfrLabels {
    pub(crate) global: Vec<krabka_blockstore::LabelPair>,
    pub(crate) contexts: HashMap<i64, Vec<krabka_blockstore::LabelPair>>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct LabelsSnapshot {
    #[prost(map = "int64, message", tag = "1")]
    pub contexts: HashMap<i64, LabelContext>,
    #[prost(map = "int64, string", tag = "2")]
    pub strings: HashMap<i64, String>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct LabelContext {
    #[prost(map = "int64, int64", tag = "1")]
    pub labels: HashMap<i64, i64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LegacyDecodeLimits {
    pub max_nodes: usize,
    pub max_path_bytes: ByteSize,
    pub max_trie_depth: usize,
}

impl Default for LegacyDecodeLimits {
    fn default() -> Self {
        Self {
            max_nodes: 500_000,
            max_path_bytes: krabka_units::mebibytes(64),
            max_trie_depth: 4_096,
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct SampleTypeConfig {
    pub(crate) units: Option<String>,
    #[serde(rename = "display-name")]
    pub(crate) display_name: Option<String>,
    pub(crate) aggregation: Option<String>,
    pub(crate) cumulative: Option<bool>,
    pub(crate) sampled: Option<bool>,
}

/// One level of the explicit trie work-stack.
pub(crate) struct TrieFrame {
    pub(crate) key: Vec<u8>,
    pub(crate) remaining: usize,
}
