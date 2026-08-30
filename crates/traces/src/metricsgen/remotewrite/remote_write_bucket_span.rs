#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct RemoteWriteBucketSpan {
    #[prost(sint32, tag = "1")]
    pub(crate) offset: i32,
    #[prost(uint32, tag = "2")]
    pub(crate) length: u32,
}
