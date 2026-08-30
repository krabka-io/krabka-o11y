use super::{LokiProtoLabelPair, LokiProtoTimestamp};

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct LokiProtoEntry {
    #[prost(message, optional, tag = "1")]
    pub(crate) timestamp: Option<LokiProtoTimestamp>,
    #[prost(string, tag = "2")]
    pub(crate) line: String,
    #[prost(message, repeated, tag = "3")]
    pub(crate) structured_metadata: Vec<LokiProtoLabelPair>,
    #[prost(message, repeated, tag = "4")]
    pub(crate) parsed: Vec<LokiProtoLabelPair>,
}
