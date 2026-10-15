use num_traits::ToPrimitive;

use super::*;

pub(crate) fn add_nested_intrinsic_columns(
    batches: Vec<RecordBatch>,
    matchers: &[SpanMatcher],
    projection_matchers: &[SpanMatcher],
) -> Result<Vec<RecordBatch>, TraceqlError> {
    let mut columns = matchers.to_vec();
    columns.extend_from_slice(projection_matchers);
    batches
        .into_iter()
        .map(|batch| add_nested_intrinsic_columns_to_batch(&batch, &columns, matchers))
        .collect()
}

pub(crate) fn add_nested_intrinsic_columns_to_batch(
    batch: &RecordBatch,
    matchers: &[SpanMatcher],
    expansion_matchers: &[SpanMatcher],
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
        .filter(|projection| schema.column_with_name(&projection.column).is_none())
        .collect::<Vec<_>>();
    if missing.is_empty() && missing_attrs.is_empty() {
        return Ok(batch.clone());
    }

    let nested = nested_intrinsic_rows(batch, expansion_matchers, &missing_attrs)?;
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
    for projection in missing_attrs {
        let column = projection.column;
        let array = nested
            .attr_columns
            .get(&column)
            .ok_or_else(|| TraceqlError::Store(format!("missing nested attr column {column}")))?
            .clone();
        fields.push(Field::new(&column, array.data_type().clone(), true));
        columns.push(array);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|err| TraceqlError::Store(format!("add nested intrinsic columns: {err}")))
}

pub(crate) fn add_span_attr_columns_to_batch(
    batch: &RecordBatch,
    wanted: &[ProjectedAttrColumn<DataType>],
) -> Result<RecordBatch, TraceqlError> {
    let schema = batch.schema();
    let mut fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    let mut columns = batch.columns().to_vec();
    for ProjectedAttrColumn {
        column_name,
        lookup_key,
        resource,
        data_type,
    } in wanted
    {
        if schema.column_with_name(column_name).is_some() {
            continue; // already a (promoted) column
        }
        let mut values = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let value = attr_values_with_resource(batch, row, *resource)?
                .into_iter()
                .find(|attribute| attribute.key == *lookup_key)
                .map(|attribute| attribute.value);
            values.push(value);
        }
        // Keep strings and booleans distinct from numbers. Numeric projections
        // may widen integers to floats, but the string "7" is never the number 7.
        let column: ArrayRef = match data_type {
            DataType::Utf8 => Arc::new(StringArray::from(
                values
                    .into_iter()
                    .map(|value| match value {
                        Some(AttrValue::Str(value)) => Some(value),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DataType::Int64 => Arc::new(Int64Array::from(
                values
                    .into_iter()
                    .map(|value| match value {
                        Some(AttrValue::Int(value)) => Some(value),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DataType::Float64 => Arc::new(Float64Array::from(
                values
                    .into_iter()
                    .map(|value| match value {
                        Some(AttrValue::Float(value)) => Some(value),
                        Some(AttrValue::Int(value)) => value.to_f64(),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DataType::Boolean => Arc::new(BooleanArray::from(
                values
                    .into_iter()
                    .map(|value| match value {
                        Some(AttrValue::Bool(value)) => Some(value),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            _ => {
                return Err(TraceqlError::Store(format!(
                    "unsupported projection type for {column_name}: {data_type:?}"
                )));
            }
        };
        fields.push(Field::new(column_name.clone(), data_type.clone(), true));
        columns.push(column);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|err| TraceqlError::Store(format!("materialize attribute columns: {err}")))
}

pub(crate) fn align_scan_batch_to_schema(
    batch: &RecordBatch,
    schema: &SchemaRef,
) -> Result<RecordBatch, TraceqlError> {
    crate::align_record_batch::align_record_batch(
        batch,
        schema,
        |field, column| {
            if column.data_type() == field.data_type() {
                Ok(column.clone())
            } else {
                cast(column, field.data_type()).map_err(|err| {
                    TraceqlError::Store(format!(
                        "cast column `{}` from {:?} to {:?}: {err}",
                        field.name(),
                        column.data_type(),
                        field.data_type()
                    ))
                })
            }
        },
        |field| {
            // Cold blocks and live batches can omit promoted columns; rebuild
            // them from the generic attribute lists before scanning.
            let attr = promoted_span_attr_from_field(field)
                .ok_or_else(|| TraceqlError::Store(format!("missing column `{}`", field.name())))?;
            promoted_attr_column(batch, &attr).map_err(|err| TraceqlError::Store(err.to_string()))
        },
        |err| TraceqlError::Store(format!("align scan batch schema: {err}")),
    )
}

/// The Arrow schema one span block was written with, read from its Parquet
/// footer.
///
/// A block's promoted attribute columns come from the ingest flags in force
/// when it was written, which need not be the flags in force now, so the block
/// is the only trustworthy source for its own schema. Only the footer is read:
/// the schema names the block's promoted attributes, and the canonical schema
/// is then rebuilt from them so it matches the batches a row-group read
/// decodes.
pub(crate) async fn block_span_schema(
    blocks: &BlockStore,
    object_key: &str,
) -> Result<SchemaRef, TraceqlError> {
    let keys = [object_key.to_string()];
    let registered = blocks
        .scan_block_keys(&keys, span_block_schema())
        .await
        .map_err(|err| block_err(&err))?;
    let table = registered
        .context
        .table(&registered.table)
        .await
        .map_err(|err| TraceqlError::Store(err.to_string()))?;
    let promoted_attrs = block_promoted_attrs(table.schema().as_arrow())
        .map_err(|err| TraceqlError::Store(err.to_string()))?;
    Ok(span_block_schema_with_promoted_attrs(&promoted_attrs))
}

pub(crate) fn filter_batches_by_matchers(
    batches: Vec<RecordBatch>,
    matchers: &[SpanMatcher],
) -> Result<Vec<RecordBatch>, TraceqlError> {
    if matchers.is_empty() {
        return Ok(batches);
    }
    batches
        .into_iter()
        .map(|batch| {
            let mask = (0..batch.num_rows())
                .map(|row| row_matches(&batch, row, matchers))
                .collect::<Result<Vec<_>, _>>()?;
            filter_record_batch(&batch, &BooleanArray::from(mask))
                .map_err(|err| TraceqlError::Store(err.to_string()))
        })
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct SpanIdentity {
    trace_id: [u8; 16],
    span_id: [u8; 8],
}

/// Collapse rows that describe the same span to the first one seen.
///
/// A scan reads the cold blocks and the hot tier over the *same* window, so a
/// span that has been flushed to a block while its hot copy is still inside the
/// live tier's retention arrives from both. The identity is
/// `(trace_id, span_id)`: OTLP makes `span_id` unique within a trace, so the
/// pair names one span across every tier and every block. It is the same key
/// [`super::deduplicate_trace_spans`] uses to merge the tiers for a by-id
/// lookup, and the same one the query frontend's search merge uses, so all
/// three read paths agree on what "the same span" means.
///
/// The span itself is never in doubt. OTLP exports a span once, when it ends,
/// so the copies are byte-for-byte the same span, whether they came from an
/// exporter retry or from a flush that left the hot copy in place. What can
/// disagree are the *derived* columns, because each tier computed them over
/// the rows it happened to hold: the nested-set columns, and the trace-level
/// ones (`rootServiceName`, `rootName`, trace start and duration).
///
/// Batches are visited in order and the first row for a key wins, so the
/// caller's ordering is the tie-break: cold blocks are appended before the hot
/// tier, and the settled copy survives -- the same preference
/// [`super::deduplicate_trace_spans`] applies on the by-id path, so a span does
/// not describe its trace one way through search and another through by-id.
/// The nested-set columns are rebuilt over the merged batch immediately after
/// this, which is also why the duplicates have to go first: two rows for one
/// span would double their parent's `childCount` and inflate the numbering for
/// the whole trace. The trace-level columns are not rebuilt here; compaction is
/// what settles those.
pub(crate) fn deduplicate_scan_batches(
    batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>, TraceqlError> {
    let mut seen: HashSet<SpanIdentity> = HashSet::new();
    let mut kept = Vec::with_capacity(batches.len());
    for batch in batches {
        let trace_ids = fixed(&batch, COL_TRACE_ID)?;
        let span_ids = fixed(&batch, COL_SPAN_ID)?;
        let mut keep = Vec::with_capacity(batch.num_rows());
        let mut any_dropped = false;
        for row in 0..batch.num_rows() {
            if trace_ids.is_null(row) || span_ids.is_null(row) {
                // An unkeyable row cannot be shown to be a duplicate of
                // anything, and dropping it would lose a span rather than a
                // copy. Keep it.
                keep.push(true);
                continue;
            }
            let mut trace_id = [0_u8; 16];
            trace_id.copy_from_slice(trace_ids.value(row));
            let mut span_id = [0_u8; 8];
            span_id.copy_from_slice(span_ids.value(row));
            let first = seen.insert(SpanIdentity { trace_id, span_id });
            any_dropped |= !first;
            keep.push(first);
        }
        if !any_dropped {
            kept.push(batch);
            continue;
        }
        let filtered = filter_record_batch(&batch, &BooleanArray::from(keep))
            .map_err(|err| TraceqlError::Store(format!("deduplicate scan batches: {err}")))?;
        if filtered.num_rows() > 0 {
            kept.push(filtered);
        }
    }
    Ok(kept)
}
