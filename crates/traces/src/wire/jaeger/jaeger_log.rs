use super::*;

#[derive(Clone, Default)]
pub(crate) struct JaegerLog {
    pub(crate) timestamp_micros: i64,
    pub(crate) fields: Vec<KeyValue>,
}
