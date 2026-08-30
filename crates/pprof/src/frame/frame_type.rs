/// One resolved stack frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub function: String,
    pub file: String,
    pub line: i32,
}
