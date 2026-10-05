use super::{
    Arc, COL_FINGERPRINT, COL_TIMESTAMP, DataType, Field, PCOL_PROFILE_TYPE, PCOL_SPAN_ID,
    PCOL_STACKTRACE_ID, PCOL_STACKTRACE_PARTITION, PCOL_TOTAL_VALUE, PCOL_TRACE_ID, PCOL_VALUE,
    PCOL_WAL_SAMPLE_IDS, Schema, SchemaRef, profile_type_dict,
};

#[must_use]
pub fn profile_samples_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COL_FINGERPRINT, DataType::UInt64, false),
        Field::new(COL_TIMESTAMP, DataType::Int64, false),
        Field::new(PCOL_PROFILE_TYPE, profile_type_dict(), false),
        Field::new(PCOL_STACKTRACE_ID, DataType::UInt64, false),
        Field::new(PCOL_VALUE, DataType::Int64, false),
        Field::new(PCOL_STACKTRACE_PARTITION, DataType::UInt64, false),
        Field::new(PCOL_TOTAL_VALUE, DataType::Int64, false),
        Field::new(PCOL_SPAN_ID, DataType::UInt64, true),
        Field::new(PCOL_TRACE_ID, DataType::Binary, true),
        Field::new(
            PCOL_WAL_SAMPLE_IDS,
            DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
            false,
        ),
    ]))
}
