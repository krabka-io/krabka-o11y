use super::LokiProtoEntry;

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct LokiProtoStream {
    #[prost(string, tag = "1")]
    pub(crate) labels: String,
    #[prost(message, repeated, tag = "2")]
    pub(crate) entries: Vec<LokiProtoEntry>,
    #[prost(uint64, tag = "3")]
    pub(crate) hash: u64,
}
