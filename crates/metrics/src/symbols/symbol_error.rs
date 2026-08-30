use super::*;

/// Errors raised by symbol-table operations.
#[derive(Debug, thiserror::Error)]
pub enum SymbolError {
    #[error("symbols[0] must be the empty string")]
    FirstNotEmpty,

    #[error("duplicate symbol `{0}`")]
    DuplicateSymbol(String),

    #[error("label_refs length {0} is not even")]
    OddRefs(usize),

    #[error("symbol ref {0} out of range (len {1})")]
    OutOfRange(u32, usize),

    #[error("duplicate label `{0}`")]
    DuplicateLabel(String),

    #[error("symbol table length {0} exceeds u32 refs")]
    TooManySymbols(usize),
}
