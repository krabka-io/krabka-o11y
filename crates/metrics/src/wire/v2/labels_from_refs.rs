use super::{Labels, SymbolTable, WireError};

pub(crate) fn labels_from_refs(table: &SymbolTable, refs: &[u32]) -> Result<Labels, WireError> {
    table
        .resolve_label_refs(refs)
        .map(Labels::from_iter)
        .map_err(|error| WireError::Invalid(error.to_string()))
}
