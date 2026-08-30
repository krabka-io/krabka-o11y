#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CaseResult {
    pub name: String,
    pub passed: bool,
    pub passed_assertions: usize,
    pub total_assertions: usize,
    pub message: String,
}
