use super::*;

pub(crate) struct ExpectBlock {
    pub(crate) lines: Vec<ExpectLine>,
    pub(crate) annotations: Vec<AnnotationExpect>,
    pub(crate) fail_message: Option<String>,
    pub(crate) range: Option<RangeExpect>,
}
