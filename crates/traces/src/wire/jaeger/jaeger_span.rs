use super::*;

#[derive(Clone, Default)]
pub(crate) struct JaegerSpan {
    pub(crate) trace_id_low: i64,
    pub(crate) trace_id_high: i64,
    pub(crate) span_id: i64,
    pub(crate) parent_span_id: i64,
    pub(crate) operation_name: String,
    pub(crate) references: Vec<JaegerRef>,
    pub(crate) start_time_micros: i64,
    pub(crate) duration_micros: i64,
    pub(crate) tags: Vec<KeyValue>,
    pub(crate) logs: Vec<JaegerLog>,
}
