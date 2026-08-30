use super::*;

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct RemoteWriteExemplar {
    #[prost(message, repeated, tag = "1")]
    pub(crate) labels: Vec<Label>,
    #[prost(double, tag = "2")]
    pub(crate) value: f64,
    #[prost(int64, tag = "3")]
    pub(crate) timestamp: i64,
}
