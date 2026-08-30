use super::{
    AttrValue, COL_DURATION, COL_KIND, COL_NAME, COL_ROOT_SERVICE_NAME, COL_STATUS_CODE,
    COL_STATUS_MESSAGE, EVENT_ATTR_PREFIX, LINK_ATTR_PREFIX, RESOURCE_ATTR_PREFIX, RecordBatch,
    Result, UnixNano, block_row_scoped_attrs, i32_value, i64_value, kind_enum_name,
    push_scoped_attr, row_attrs, status_enum_name, string_value,
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
    // attrs are stored as `attr.__event.<k>` / `attr.__link.<k>` columns; they
    // belong to a child scope the per-span span distribution must not surface
    // as `span.__event.<k>` / `span.__link.<k>`, so drop them here.
    for (key, value) in row_attrs(batch, row)? {
        if key.starts_with(EVENT_ATTR_PREFIX) || key.starts_with(LINK_ATTR_PREFIX) {
            continue;
        }
        push_scoped_attr(&mut attrs, "span", &key, &value);
        raw_span_attrs.push((key, value));
    }
    // Block attribute-list columns carry the remaining span + resource attrs.
    for (key, value) in block_row_scoped_attrs(batch, row)? {
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
            push_scoped_attr(&mut attrs, "span", &key, &value);
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
    })
}
