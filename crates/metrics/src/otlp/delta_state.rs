use super::*;

#[derive(Clone, Debug, Default)]
pub(crate) struct DeltaState {
    pub(crate) start_time_unix_nano: u64,
    pub(crate) value: f64,
}
