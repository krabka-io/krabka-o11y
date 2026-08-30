use super::*;

pub(crate) fn add_nested_intrinsic_columns_to_batch(
    batch: &RecordBatch,
    matchers: &[SpanMatcher],
) -> Result<RecordBatch, TraceqlError> {
    let schema = batch.schema();
    let missing = [
        COL_EVENT_NAME,
        COL_EVENT_TIME_SINCE_START,
        COL_LINK_TRACE_ID,
        COL_LINK_SPAN_ID,
    ]
    .into_iter()
    .filter(|name| schema.column_with_name(name).is_none())
    .collect::<Vec<_>>();
    let missing_attrs = nested_attr_columns(matchers)
        .into_iter()
        .filter(|(column, _)| schema.column_with_name(column).is_none())
        .collect::<Vec<_>>();
    if missing.is_empty() && missing_attrs.is_empty() {
        return Ok(batch.clone());
    }

    let nested = nested_intrinsic_rows(batch, matchers, &missing_attrs)?;
    let mut fields = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let mut columns = batch
        .columns()
        .iter()
        .map(|column| {
            take(column.as_ref(), &nested.indices, None)
                .map_err(|err| TraceqlError::Store(format!("expand nested intrinsic rows: {err}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for name in missing {
        match name {
            COL_EVENT_NAME => {
                fields.push(Field::new(COL_EVENT_NAME, DataType::Utf8, true));
                columns.push(nested.event_name.clone());
            }
            COL_EVENT_TIME_SINCE_START => {
                fields.push(Field::new(
                    COL_EVENT_TIME_SINCE_START,
                    DataType::Int64,
                    true,
                ));
                columns.push(nested.event_time_since_start.clone());
            }
            COL_LINK_TRACE_ID => {
                fields.push(Field::new(
                    COL_LINK_TRACE_ID,
                    DataType::FixedSizeBinary(16),
                    true,
                ));
                columns.push(nested.link_trace_id.clone());
            }
            COL_LINK_SPAN_ID => {
                fields.push(Field::new(
                    COL_LINK_SPAN_ID,
                    DataType::FixedSizeBinary(8),
                    true,
                ));
                columns.push(nested.link_span_id.clone());
            }
            _ => {}
        }
    }
    for (column, _) in missing_attrs {
        fields.push(Field::new(&column, DataType::Utf8, true));
        columns.push(
            nested
                .attr_columns
                .get(&column)
                .ok_or_else(|| TraceqlError::Store(format!("missing nested attr column {column}")))?
                .clone(),
        );
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|err| TraceqlError::Store(format!("add nested intrinsic columns: {err}")))
}
