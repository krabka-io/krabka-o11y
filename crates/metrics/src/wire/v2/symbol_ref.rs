use super::*;

pub(crate) fn symbol_ref(table: &SymbolTable, index: u32) -> Result<String, WireError> {
    table
        .resolve(index)
        .map(str::to_string)
        .ok_or_else(|| WireError::Invalid(format!("symbol ref {index} out of range")))
}
