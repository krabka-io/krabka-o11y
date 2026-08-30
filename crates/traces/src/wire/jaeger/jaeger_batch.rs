use super::{JaegerProcess, JaegerSpan};

#[derive(Clone, Default)]
pub(crate) struct JaegerBatch {
    pub(crate) process: JaegerProcess,
    pub(crate) spans: Vec<JaegerSpan>,
}
