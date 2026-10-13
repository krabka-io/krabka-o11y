use super::{Labels, SymbolTable, WireError};

pub(crate) fn labels_from_refs(table: &SymbolTable, refs: &[u32]) -> Result<Labels, WireError> {
    table
        .resolve_label_refs(refs)
        .map(|labels| {
            labels
                .into_iter()
                .map(|label| krabka_blockstore::LabelPair::new(label.name, label.value))
                .collect()
        })
        .map_err(|error| WireError::Invalid(error.to_string()))
}
