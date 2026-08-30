use super::*;

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct LokiProtoLabelPair {
    #[prost(string, tag = "1")]
    pub(crate) name: String,
    #[prost(string, tag = "2")]
    pub(crate) value: String,
}
