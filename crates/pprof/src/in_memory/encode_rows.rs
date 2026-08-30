use super::{
    Arc, ArrayRef, BinaryBuilder, Int32Type, Int64Builder, ProfileError, RecordBatch, SampleRow,
    StringDictionaryBuilder, UInt64Builder, profile_samples_schema,
};

pub(crate) fn encode_rows(rows: &[&SampleRow]) -> Result<RecordBatch, ProfileError> {
    let mut fp = UInt64Builder::new();
    let mut ts = Int64Builder::new();
    let mut profile_type = StringDictionaryBuilder::<Int32Type>::new();
    let mut stacktrace_id = UInt64Builder::new();
    let mut value = Int64Builder::new();
    let mut partition = UInt64Builder::new();
    let mut total_value = Int64Builder::new();
    let mut span_id = UInt64Builder::new();
    let mut trace_id = BinaryBuilder::new();

    for row in rows {
        fp.append_value(row.fingerprint);
        ts.append_value(row.timestamp_ms);
        profile_type
            .append(&row.profile_type)
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        stacktrace_id.append_value(u64::from(row.stacktrace_id));
        value.append_value(row.value);
        partition.append_value(row.partition);
        total_value.append_value(row.total_value);
        match row.span_id {
            Some(value) => span_id.append_value(value),
            None => span_id.append_null(),
        }
        match &row.trace_id {
            Some(value) => trace_id.append_value(value),
            None => trace_id.append_null(),
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(fp.finish()),
        Arc::new(ts.finish()),
        Arc::new(profile_type.finish()),
        Arc::new(stacktrace_id.finish()),
        Arc::new(value.finish()),
        Arc::new(partition.finish()),
        Arc::new(total_value.finish()),
        Arc::new(span_id.finish()),
        Arc::new(trace_id.finish()),
    ];
    RecordBatch::try_new(profile_samples_schema(), columns)
        .map_err(|err| ProfileError::Store(err.to_string()))
}
