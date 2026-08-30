use super::NativeHistogram;

#[derive(Clone, Debug, Default)]
pub(crate) struct DeltaHistogramState {
    pub(crate) start_time_unix_nano: u64,
    pub(crate) value: Option<NativeHistogram>,
}
