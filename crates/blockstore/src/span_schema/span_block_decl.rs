use super::*;

/// Span block declaration used by generic schema validation.
#[must_use]
pub fn span_block_decl() -> BlockSchema {
    BlockSchema {
        required: vec![
            RequiredColumn::new(SCOL_TRACE_ID, DataType::FixedSizeBinary(16), false),
            RequiredColumn::new(SCOL_START_NANO, DataType::Int64, false),
        ],
        sort_key: vec![SCOL_TRACE_ID.to_string(), SCOL_START_NANO.to_string()],
    }
}
