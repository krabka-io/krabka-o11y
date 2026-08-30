#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeSymbol {
    pub function: String,
    pub file: String,
    pub line: i32,
}
