use super::LokiProtoStream;

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct LokiProtoPushRequest {
    #[prost(message, repeated, tag = "1")]
    pub(crate) streams: Vec<LokiProtoStream>,
}
