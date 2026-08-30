use super::*;

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub(crate) labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub(crate) samples: Vec<Sample>,
    #[prost(message, repeated, tag = "3")]
    pub(crate) exemplars: Vec<RemoteWriteExemplar>,
    #[prost(message, repeated, tag = "4")]
    pub(crate) histograms: Vec<Histogram>,
}
