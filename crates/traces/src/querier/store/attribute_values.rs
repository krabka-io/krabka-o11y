use super::{
    ATTR_PREFIX, Array, AttrValue, Attribute, BTreeMap, BTreeSet, COL_CHILD_COUNT, COL_DURATION,
    COL_INSTRUMENTATION_NAME, COL_INSTRUMENTATION_VERSION, COL_KIND, COL_NAME, COL_NS_LEFT,
    COL_NS_RIGHT, COL_PARENT_ID, COL_PARENT_SPAN_ID, COL_ROOT_SERVICE_NAME, COL_ROOT_SPAN_NAME,
    COL_SPAN_ID, COL_STATUS_CODE, COL_STATUS_MESSAGE, COL_TRACE_DURATION, COL_TRACE_ID, DataType,
    EventRef, INSTRUMENTATION_ATTR_PREFIX, LinkRef, MatchCmp, MatchValue, RESOURCE_ATTR_PREFIX,
    RecordBatch, SCOL_EVENTS, SCOL_LINKS, SpanMatcher, StructArray, TagScope, TagScopeTags, Time,
    TimeExt, TraceqlError, TypedValue, attr_values_match, block_row_attrs_where, bool_array_value,
    bytes_to_hex, fixed, fixed_array_value, float64_array_value, insert_i32_value,
    insert_i64_value, insert_string_value, int64_array_value, nested_string_attrs,
    nullable_fixed_value, optional_list_column, root_service_matches, string_array_value,
    string_matches, string_value, struct_fixed_field, struct_int64_field, struct_list_field,
    struct_string_field, tag_scope_key, typed_value_parts,
};

/// One attribute comparison against a row: the attribute key, how to compare,
/// the value to compare with, and whether resource attributes take part.
#[derive(Clone, Copy)]
pub(crate) struct AttrMatch<'a> {
    pub(crate) key: &'a str,
    pub(crate) op: MatchCmp,
    pub(crate) expected: &'a MatchValue,
    pub(crate) resource: ResourceAttrs,
}

pub(crate) fn attr_values_with_resource(
    batch: &RecordBatch,
    row: usize,
    resource: ResourceAttrs,
) -> Result<Vec<Attribute>, TraceqlError> {
    let packed = block_row_attrs_where(batch, row, |key| {
        resource == ResourceAttrs::Include || !key.starts_with(RESOURCE_ATTR_PREFIX)
    })?;
    let mut out = Vec::new();
    let schema = batch.schema();
    let fields = schema.fields();
    for idx in 0..fields.len() {
        let field = &fields[idx];
        let Some(key) = field.name().strip_prefix(ATTR_PREFIX) else {
            continue;
        };
        if packed.iter().any(|attribute| attribute.key == key) {
            continue;
        }
        let col = batch.column(idx);
        if col.is_null(row) {
            continue;
        }
        let value = match field.data_type() {
            DataType::Utf8 => AttrValue::Str(string_array_value(col.as_ref(), row)?),
            DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Utf8 => {
                AttrValue::Str(string_array_value(col.as_ref(), row)?)
            }
            DataType::Int64 => AttrValue::Int(int64_array_value(col.as_ref(), row)?),
            DataType::Float64 => AttrValue::Float(float64_array_value(col.as_ref(), row)?),
            DataType::Boolean => AttrValue::Bool(bool_array_value(col.as_ref(), row)?),
            _ => continue,
        };
        out.push(Attribute::new(key, value));
    }
    out.extend(packed);
    Ok(out)
}

pub(crate) fn batch_attr_matches(
    batch: &RecordBatch,
    row: usize,
    key: &str,
    op: MatchCmp,
    expected: &MatchValue,
) -> Result<bool, TraceqlError> {
    batch_attr_matches_with_resource(
        batch,
        row,
        AttrMatch {
            key,
            op,
            expected,
            resource: ResourceAttrs::Exclude,
        },
    )
}

pub(crate) fn batch_attr_matches_with_resource(
    batch: &RecordBatch,
    row: usize,
    attr_match: AttrMatch<'_>,
) -> Result<bool, TraceqlError> {
    let AttrMatch {
        key,
        op,
        expected,
        resource,
    } = attr_match;
    let attrs = attr_values_with_resource(batch, row, resource)?;
    let values = attrs
        .iter()
        .filter(|attribute| attribute.key == key)
        .map(|attribute| &attribute.value)
        .collect::<Vec<_>>();
    Ok(attr_values_match(&values, op, expected))
}

#[derive(Default)]
pub(crate) struct ColdAttributeTagNames {
    pub(crate) resource: BTreeSet<String>,
    pub(crate) span: BTreeSet<String>,
    pub(crate) event: BTreeSet<String>,
    pub(crate) link: BTreeSet<String>,
    pub(crate) instrumentation: BTreeSet<String>,
}

pub(crate) fn collect_attribute_tag_names(
    batch: &RecordBatch,
    names: &mut ColdAttributeTagNames,
) -> Result<(), TraceqlError> {
    for row in 0..batch.num_rows() {
        for attribute in attr_values_with_resource(batch, row, ResourceAttrs::Include)? {
            if let Some(key) = attribute.key.strip_prefix(RESOURCE_ATTR_PREFIX) {
                names.resource.insert(key.to_string());
            } else if let Some(key) = attribute.key.strip_prefix(INSTRUMENTATION_ATTR_PREFIX) {
                names.instrumentation.insert(key.to_string());
            } else {
                names.span.insert(attribute.key);
            }
        }
        for event in event_values(batch, row)? {
            names
                .event
                .extend(event.attributes.into_iter().map(|attribute| attribute.key));
        }
        for link in link_values(batch, row)? {
            names
                .link
                .extend(link.attributes.into_iter().map(|attribute| attribute.key));
        }
    }
    Ok(())
}

pub(crate) fn collect_intrinsic_value(
    batch: &RecordBatch,
    row: usize,
    tag: &str,
    values: &mut BTreeSet<TypedValue>,
) -> Result<(), TraceqlError> {
    match tag {
        "span:duration" => {
            insert_i64_value(batch, row, values, "duration", COL_DURATION)?;
        }
        "span:id" => {
            values.insert(TypedValue::new(
                "string",
                bytes_to_hex(fixed(batch, COL_SPAN_ID)?.value(row)),
            ));
        }
        "span:kind" => {
            insert_i32_value(batch, row, values, COL_KIND)?;
        }
        "span:name" => {
            insert_string_value(batch, row, values, COL_NAME)?;
        }
        "span:childCount" => {
            insert_i32_value(batch, row, values, COL_CHILD_COUNT)?;
        }
        "span:parentID" => {
            if let Some(parent_id) = nullable_fixed_value::<8>(batch, COL_PARENT_SPAN_ID, row)? {
                values.insert(TypedValue::new("string", bytes_to_hex(&parent_id)));
            }
        }
        "span:status" => {
            insert_i32_value(batch, row, values, COL_STATUS_CODE)?;
        }
        "span:statusMessage" => {
            let message = string_value(batch, COL_STATUS_MESSAGE, row)?;
            if !message.is_empty() {
                values.insert(TypedValue::new("string", message));
            }
        }
        "span:nestedSetLeft" => {
            insert_i32_value(batch, row, values, COL_NS_LEFT)?;
        }
        "span:nestedSetParent" | "span:Parent" => {
            insert_i32_value(batch, row, values, COL_PARENT_ID)?;
        }
        "span:nestedSetRight" => {
            insert_i32_value(batch, row, values, COL_NS_RIGHT)?;
        }
        "trace:duration" => {
            insert_i64_value(batch, row, values, "duration", COL_TRACE_DURATION)?;
        }
        "trace:id" => {
            values.insert(TypedValue::new(
                "string",
                bytes_to_hex(fixed(batch, COL_TRACE_ID)?.value(row)),
            ));
        }
        "trace:rootName" => {
            insert_string_value(batch, row, values, COL_ROOT_SPAN_NAME)?;
        }
        "trace:rootService" => {
            insert_string_value(batch, row, values, COL_ROOT_SERVICE_NAME)?;
        }
        "instrumentation:name" => {
            insert_string_value(batch, row, values, COL_INSTRUMENTATION_NAME)?;
        }
        "instrumentation:version" => {
            insert_string_value(batch, row, values, COL_INSTRUMENTATION_VERSION)?;
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn instrumentation_matches(
    batch: &RecordBatch,
    row: usize,
    matcher: &SpanMatcher,
) -> Result<bool, TraceqlError> {
    Ok(match matcher.key.as_str() {
        "name" | "instrumentation:name" => string_matches(
            &string_value(batch, COL_INSTRUMENTATION_NAME, row)?,
            matcher.op,
            &matcher.value,
        ),
        "version" | "instrumentation:version" => string_matches(
            &string_value(batch, COL_INSTRUMENTATION_VERSION, row)?,
            matcher.op,
            &matcher.value,
        ),
        _ => batch_attr_matches(
            batch,
            row,
            &format!("{INSTRUMENTATION_ATTR_PREFIX}{}", matcher.key),
            matcher.op,
            &matcher.value,
        )?,
    })
}

pub(crate) fn intrinsic_values_from_batches(
    tag: &str,
    batches: &[RecordBatch],
) -> Result<Vec<TypedValue>, TraceqlError> {
    let mut values = BTreeSet::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            collect_intrinsic_value(batch, row, tag, &mut values)?;
        }
    }
    Ok(values
        .into_iter()
        .map(|value| TypedValue::new(value.type_, value.value))
        .collect())
}

pub(crate) fn link_values(batch: &RecordBatch, row: usize) -> Result<Vec<LinkRef>, TraceqlError> {
    let Some(links) = optional_list_column(batch, SCOL_LINKS)? else {
        return Ok(Vec::new());
    };
    if links.is_null(row) {
        return Ok(Vec::new());
    }
    let row_links = links.value(row);
    let row_links = row_links
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            TraceqlError::Store(format!("nested column `{SCOL_LINKS}` row is not a struct"))
        })?;
    let trace_ids = struct_fixed_field(row_links, 0, SCOL_LINKS)?;
    let span_ids = struct_fixed_field(row_links, 1, SCOL_LINKS)?;
    let attr_keys = struct_list_field(row_links, 2, SCOL_LINKS)?;
    let attr_values = struct_list_field(row_links, 3, SCOL_LINKS)?;

    let mut out = Vec::new();
    for idx in 0..row_links.len() {
        if row_links.is_null(idx) {
            continue;
        }
        out.push(LinkRef {
            trace_id: fixed_array_value::<16>(trace_ids, idx, SCOL_LINKS)?,
            span_id: fixed_array_value::<8>(span_ids, idx, SCOL_LINKS)?,
            attributes: nested_typed_attrs(row_links, idx)?.unwrap_or(nested_string_attrs(
                attr_keys,
                attr_values,
                idx,
            )?),
        });
    }
    Ok(out)
}

pub(crate) fn merge_dynamic_scope(
    by_scope: &mut BTreeMap<&'static str, TagScopeTags>,
    requested: Option<TagScope>,
    scope: TagScope,
    tags: BTreeSet<String>,
) {
    if requested.is_some_and(|requested| requested != scope) || tags.is_empty() {
        return;
    }
    let scope_tags = by_scope
        .entry(tag_scope_key(scope))
        .or_insert_with(|| TagScopeTags {
            scope,
            tags: BTreeSet::new(),
        });
    scope_tags.tags.extend(tags);
}

pub(crate) fn merge_static_scope(
    by_scope: &mut BTreeMap<&'static str, TagScopeTags>,
    requested: Option<TagScope>,
    scope: TagScope,
    tags: &[&str],
) {
    if requested.is_some_and(|requested| requested != scope) {
        return;
    }
    let scope_tags = by_scope
        .entry(tag_scope_key(scope))
        .or_insert_with(|| TagScopeTags {
            scope,
            tags: BTreeSet::new(),
        });
    scope_tags
        .tags
        .extend(tags.iter().map(|tag| (*tag).to_string()));
}

pub(crate) fn resource_attr_values(
    batch: &RecordBatch,
    row: usize,
) -> Result<Vec<Attribute>, TraceqlError> {
    Ok(
        attr_values_with_resource(batch, row, ResourceAttrs::Include)?
            .into_iter()
            .filter_map(|attribute| {
                attribute
                    .key
                    .strip_prefix(RESOURCE_ATTR_PREFIX)
                    .map(|key| Attribute::new(key, attribute.value))
            })
            .collect(),
    )
}

/// Whether a row's attribute read includes its `resource.`-prefixed
/// attributes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResourceAttrs {
    Include,
    Exclude,
}

pub(crate) fn resource_matches(
    batch: &RecordBatch,
    row: usize,
    matcher: &SpanMatcher,
) -> Result<bool, TraceqlError> {
    Ok(match matcher.key.as_str() {
        "service.name" => {
            root_service_matches(&string_value(batch, COL_ROOT_SERVICE_NAME, row)?, matcher)
        }
        _ => batch_attr_matches_with_resource(
            batch,
            row,
            AttrMatch {
                key: &format!("{RESOURCE_ATTR_PREFIX}{}", matcher.key),
                op: matcher.op,
                expected: &matcher.value,
                resource: ResourceAttrs::Include,
            },
        )?,
    })
}

pub(crate) fn event_values(batch: &RecordBatch, row: usize) -> Result<Vec<EventRef>, TraceqlError> {
    let Some(events) = optional_list_column(batch, SCOL_EVENTS)? else {
        return Ok(Vec::new());
    };
    if events.is_null(row) {
        return Ok(Vec::new());
    }
    let row_events = events.value(row);
    let row_events = row_events
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            TraceqlError::Store(format!("nested column `{SCOL_EVENTS}` row is not a struct"))
        })?;
    let names = struct_string_field(row_events, 0, SCOL_EVENTS)?;
    let times = struct_int64_field(row_events, 1, SCOL_EVENTS)?;
    let attr_keys = struct_list_field(row_events, 2, SCOL_EVENTS)?;
    let attr_values = struct_list_field(row_events, 3, SCOL_EVENTS)?;

    let mut out = Vec::new();
    for idx in 0..row_events.len() {
        if row_events.is_null(idx) {
            continue;
        }
        let name = if names.is_null(idx) {
            String::new()
        } else {
            string_array_value(names, idx)?
        };
        let time_since_start = if times.is_null(idx) {
            <Time as TimeExt>::ZERO
        } else {
            Time::from_nanos(times.value(idx))
        };
        out.push(EventRef {
            time_since_start,
            name,
            attributes: nested_typed_attrs(row_events, idx)?.unwrap_or(nested_string_attrs(
                attr_keys,
                attr_values,
                idx,
            )?),
        });
    }
    Ok(out)
}

pub(super) fn nested_typed_attrs(
    structs: &StructArray,
    row: usize,
) -> Result<Option<Vec<Attribute>>, TraceqlError> {
    let Some(payload) = structs.column_by_name("attr_typed") else {
        return Ok(None);
    };
    if payload.is_null(row) {
        return Ok(None);
    }
    let payload = string_array_value(payload.as_ref(), row)?;
    let attrs: Vec<krabka_blockstore::SpanAttr> =
        serde_json::from_str(&payload).map_err(|error| {
            TraceqlError::Store(format!("invalid typed nested attributes: {error}"))
        })?;
    Ok(Some(
        attrs
            .into_iter()
            .flat_map(|attr| {
                use krabka_blockstore::AttrValue as BlockValue;

                use super::AttrValue;
                let values = match attr.value {
                    BlockValue::Unsupported(value) => {
                        return vec![Attribute::new(
                            attr.key,
                            super::AttrValue::Unsupported(value),
                        )];
                    }
                    BlockValue::Str(values) => {
                        values.into_iter().map(AttrValue::Str).collect::<Vec<_>>()
                    }
                    BlockValue::Int(values) => values.into_iter().map(AttrValue::Int).collect(),
                    BlockValue::Double(values) => {
                        values.into_iter().map(AttrValue::Float).collect()
                    }
                    BlockValue::Bool(values) => values.into_iter().map(AttrValue::Bool).collect(),
                };
                if attr.is_array {
                    vec![Attribute::new(attr.key, AttrValue::Array(values))]
                } else {
                    values
                        .into_iter()
                        .map(|value| Attribute::new(attr.key.clone(), value))
                        .collect()
                }
            })
            .collect(),
    ))
}

/// The tag a tag-values request names, and the key the trace index holds it
/// under.
#[derive(Clone, Copy)]
pub(crate) struct RequestedTag<'a> {
    pub(crate) tag: &'a str,
    pub(crate) index_tag: &'a str,
}

pub(crate) fn collect_attribute_tag_values(
    batch: &RecordBatch,
    requested: RequestedTag<'_>,
    values: &mut BTreeSet<super::TypedValue>,
) -> Result<(), TraceqlError> {
    let RequestedTag { tag, index_tag } = requested;
    for row in 0..batch.num_rows() {
        for attribute in attr_values_with_resource(batch, row, ResourceAttrs::Include)? {
            let key = attribute.key;
            let matches = if let Some(key) = key.strip_prefix(RESOURCE_ATTR_PREFIX) {
                [tag, index_tag].contains(&key)
            } else if let Some(key) = key.strip_prefix(INSTRUMENTATION_ATTR_PREFIX) {
                let requested = tag.strip_prefix("instrumentation.").unwrap_or(tag);
                let indexed = index_tag
                    .strip_prefix(INSTRUMENTATION_ATTR_PREFIX)
                    .unwrap_or(requested);
                [requested, indexed].contains(&key)
            } else {
                [tag, index_tag].contains(&key.as_str())
            };
            if matches {
                values.extend(typed_value_parts(&attribute.value));
            }
        }
        for event in event_values(batch, row)? {
            for attribute in event.attributes {
                if attribute.key == tag || attribute.key == index_tag {
                    values.extend(typed_value_parts(&attribute.value));
                }
            }
        }
        for link in link_values(batch, row)? {
            for attribute in link.attributes {
                if attribute.key == tag || attribute.key == index_tag {
                    values.extend(typed_value_parts(&attribute.value));
                }
            }
        }
    }
    Ok(())
}
