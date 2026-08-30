#[derive(Clone, Default)]
pub(crate) struct JaegerRef {
    pub(crate) ref_type: i32,
    pub(crate) trace_id_low: i64,
    pub(crate) trace_id_high: i64,
    pub(crate) span_id: i64,
}
