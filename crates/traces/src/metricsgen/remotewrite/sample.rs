
#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct Sample {
    #[prost(double, tag = "1")]
    pub(crate) value: f64,
    #[prost(int64, tag = "2")]
    pub(crate) timestamp: i64,
}
