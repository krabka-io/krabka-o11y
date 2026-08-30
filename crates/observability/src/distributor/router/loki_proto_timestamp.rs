use super::*;

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct LokiProtoTimestamp {
    #[prost(int64, tag = "1")]
    pub(crate) seconds: i64,
    #[prost(int32, tag = "2")]
    pub(crate) nanos: i32,
}
