use super::{
    Array, AsArray, AttrValue, BTreeMap, COL_DURATION, COL_KIND, COL_NAME, COL_ROOT_SERVICE_NAME,
    COL_STATUS_CODE, COL_STATUS_MESSAGE, DataType, EVENT_ATTR_PREFIX, LINK_ATTR_PREFIX,
    RESOURCE_ATTR_PREFIX, RecordBatch, Result, UnixNano, Value, block_row_scoped_attrs, i32_value,
    i64_value, kind_enum_name, push_scoped_attr, row_attrs, status_enum_name, string_value,
};

/// One scanned span row, projected into the values the compare needs.
///
/// The values are the span start time, every scoped attribute, and the
/// selection-evaluable intrinsics. The compare uses the attributes for the
/// distribution and the intrinsics for group membership.
pub(crate) struct CompareRow {
    pub(crate) ts: UnixNano,
    /// Fully-scoped attribute key → display value, deduplicated. Example keys:
    /// `span.http.method` and `resource.service.name`.
    pub(crate) attrs: Vec<(String, String)>,
    /// Raw span or resource attribute key → typed values, with repeated
    /// attributes allowed. The compare uses these values to evaluate the
    /// selection's attribute comparisons.
    pub(crate) raw_span_attrs: Vec<(String, AttrValue)>,
    pub(crate) raw_resource_attrs: Vec<(String, AttrValue)>,
    pub(crate) name: Option<String>,
    pub(crate) status_code: Option<i32>,
    pub(crate) status_message: Option<String>,
    pub(crate) kind: Option<i32>,
    pub(crate) duration: Option<i64>,
    pub(crate) columns: BTreeMap<String, Value>,
}

/// Projects one scanned span row into a `CompareRow`.
///
/// The projection holds every scoped attribute for the distribution, plus the
/// selection-evaluable intrinsics. Attributes come from two sources, the same
/// two that `row_attrs` and `block_row_attrs` read. The first source is the
/// promoted `attr.<key>` schema columns, which are span-scoped. The second
/// source is the block attribute-list columns `attr_keys` and `attr_value*`,
/// where a `__resource.` prefix on the key marks a resource attribute. This
/// function reports the root-service column as `resource.service.name`.
pub(crate) fn compare_row(batch: &RecordBatch, row: usize, ts: UnixNano) -> Result<CompareRow> {
    let mut attrs: Vec<(String, String)> = Vec::new();
    let mut raw_span_attrs: Vec<(String, AttrValue)> = Vec::new();
    let mut raw_resource_attrs: Vec<(String, AttrValue)> = Vec::new();

    // Promoted `attr.<key>` columns are span-scoped attributes. Event/link
    // attrs stay available to the typed evaluator, but their child scope must
    // not enter the per-span distribution as span attributes.
    let packed_attrs = block_row_scoped_attrs(batch, row)?;
    for (key, value) in row_attrs(batch, row)? {
        if packed_attrs
            .iter()
            .any(|(packed_key, _)| packed_key == &key)
        {
            continue;
        }
        if !key.starts_with(EVENT_ATTR_PREFIX) && !key.starts_with(LINK_ATTR_PREFIX) {
            push_scoped_attr(&mut attrs, "span", &key, &value);
        }
        raw_span_attrs.push((key, value));
    }
    // Block attribute-list columns carry the remaining span + resource attrs.
    for (key, value) in packed_attrs {
        if let Some(stripped) = key.strip_prefix(RESOURCE_ATTR_PREFIX) {
            // The per-span `__resource.service.name` block attr would
            // double-count `resource.service.name`, which the rest of the
            // engine defines as the TRACE-ROOT service (COL_ROOT_SERVICE_NAME,
            // emitted below). Skip it so the root column is the sole emitter.
            if stripped == "service.name" {
                continue;
            }
            push_scoped_attr(&mut attrs, "resource", stripped, &value);
            raw_resource_attrs.push((stripped.to_string(), value));
        } else {
            if !key.starts_with(EVENT_ATTR_PREFIX) && !key.starts_with(LINK_ATTR_PREFIX) {
                push_scoped_attr(&mut attrs, "span", &key, &value);
            }
            raw_span_attrs.push((key, value));
        }
    }
    // The root-service-name column is the canonical `resource.service.name`.
    if let Some(service) = string_value(batch, COL_ROOT_SERVICE_NAME, row)
        && !service.is_empty()
    {
        push_scoped_attr(
            &mut attrs,
            "resource",
            "service.name",
            &AttrValue::Str(service.clone()),
        );
        raw_resource_attrs.push(("service.name".to_string(), AttrValue::Str(service)));
    }

    let name = string_value(batch, COL_NAME, row);
    let status_code = i32_value(batch, COL_STATUS_CODE, row).ok();
    let status_message = string_value(batch, COL_STATUS_MESSAGE, row);
    let kind = i32_value(batch, COL_KIND, row).ok();
    let duration = i64_value(batch, COL_DURATION, row).ok();

    // Intrinsics participate in the value distribution too (Tempo emits e.g.
    // `name` and `status` distributions): name as-is, status/kind as their
    // TraceQL enum names.
    if let Some(name) = &name {
        attrs.push(("name".to_string(), name.clone()));
    }
    if let Some(code) = status_code {
        attrs.push(("status".to_string(), status_enum_name(code).to_string()));
    }
    if let Some(code) = kind {
        attrs.push(("kind".to_string(), kind_enum_name(code).to_string()));
    }

    let mut columns = BTreeMap::new();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        let array = batch.column(index);
        if array.is_null(row) {
            continue;
        }
        let value = match array.data_type() {
            DataType::Int64 => Value::Int(
                array
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .value(row),
            ),
            DataType::Int32 => Value::Int(i64::from(
                array
                    .as_primitive::<arrow::datatypes::Int32Type>()
                    .value(row),
            )),
            DataType::Float64 => Value::Float(
                array
                    .as_primitive::<arrow::datatypes::Float64Type>()
                    .value(row),
            ),
            DataType::Boolean => Value::Bool(array.as_boolean().value(row)),
            DataType::FixedSizeBinary(width) => {
                let hex = super::bytes_to_hex(array.as_fixed_size_binary().value(row));
                Value::Str(if *width == 16 {
                    hex.trim_start_matches('0').to_owned()
                } else {
                    hex
                })
            }
            _ => match super::string_array_value(array.as_ref(), row) {
                Some(value) => Value::Str(value),
                None => continue,
            },
        };
        columns.insert(field.name().clone(), value);
    }

    attrs.sort();
    attrs.dedup();
    Ok(CompareRow {
        ts,
        attrs,
        raw_span_attrs,
        raw_resource_attrs,
        name,
        status_code,
        status_message,
        kind,
        duration,
        columns,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::FixedSizeBinaryBuilder,
        datatypes::{Field, Schema},
    };
    use assert2::assert;

    use super::*;

    #[test]
    fn intrinsic_ids_keep_span_padding_and_trim_only_trace_padding() {
        let mut traces = FixedSizeBinaryBuilder::new(16);
        let mut spans = FixedSizeBinaryBuilder::new(8);
        traces
            .append_value([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10])
            .unwrap();
        spans.append_value([0, 0, 0, 0, 0, 0, 0, 1]).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(
                    super::super::COL_TRACE_ID,
                    DataType::FixedSizeBinary(16),
                    false,
                ),
                Field::new(
                    super::super::COL_SPAN_ID,
                    DataType::FixedSizeBinary(8),
                    false,
                ),
            ])),
            vec![Arc::new(traces.finish()), Arc::new(spans.finish())],
        )
        .unwrap();
        let row = compare_row(&batch, 0, UnixNano(0)).unwrap();
        assert!(row.columns[super::super::COL_TRACE_ID] == Value::Str("a".into()));
        assert!(row.columns[super::super::COL_SPAN_ID] == Value::Str("0000000000000001".into()));
    }
}
