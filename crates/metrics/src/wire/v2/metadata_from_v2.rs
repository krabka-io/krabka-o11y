use super::{DecodedMetadata, Labels, SymbolTable, WireError, metadata_type, pb, symbol_ref};

pub(crate) fn metadata_from_v2(
    table: &SymbolTable,
    labels: &Labels,
    metadata: &pb::v2::Metadata,
) -> Result<DecodedMetadata, WireError> {
    Ok(DecodedMetadata {
        metric_family_name: labels.get("__name__").unwrap_or_default().to_string(),
        metric_type: metadata_type(metadata.r#type),
        help: symbol_ref(table, metadata.help_ref)?,
        unit: symbol_ref(table, metadata.unit_ref)?,
    })
}
