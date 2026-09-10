use super::{BlockSchema, DataType, RequiredColumn, SCOL_SPAN_ID, SCOL_START_NANO, SCOL_TRACE_ID};

/// Span block declaration used by generic schema validation.
#[must_use]
pub fn span_block_decl() -> BlockSchema {
    BlockSchema {
        required: vec![
            RequiredColumn::new(SCOL_TRACE_ID, DataType::FixedSizeBinary(16), false),
            RequiredColumn::new(SCOL_START_NANO, DataType::Int64, false),
        ],
        sort_key: vec![SCOL_TRACE_ID.to_string(), SCOL_START_NANO.to_string()],
        // The span id, and only the span id. The trace id leads the sort key,
        // so min/max prunes it and the traces index carries its own trace-id
        // bloom besides; but span ids are random bytes scattered through
        // every row group, and an equality on one has nothing else to go on.
        bloom_columns: vec![SCOL_SPAN_ID.to_string()],
    }
}
