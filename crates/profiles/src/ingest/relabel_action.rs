#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelabelAction {
    Replace,
    Keep,
    Drop,
}
