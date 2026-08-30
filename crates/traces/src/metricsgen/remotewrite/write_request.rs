use super::*;

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub(crate) timeseries: Vec<TimeSeries>,
}
