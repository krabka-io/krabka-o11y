use super::{AnnotationExpect, ExpectLine, ExpectedFailure, RangeExpect};

pub(crate) struct ExpectBlock {
    pub(crate) lines: Vec<ExpectLine>,
    pub(crate) annotations: Vec<AnnotationExpect>,
    pub(crate) fail_message: Option<ExpectedFailure>,
    pub(crate) range: Option<RangeExpect>,
    pub(crate) ordered: bool,
}
