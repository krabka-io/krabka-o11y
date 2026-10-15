use super::*;
use crate::span::nested_set::BatchNestedSets;
use crate::span::nested_set::SpanIdColumns;
use crate::span::nested_set::batch_nested_sets;
use krabka_blockstore::CycleSpans;
use krabka_blockstore::SpanNode;
use krabka_blockstore::assign_nested_set;

pub(crate) fn append_nested_attr(
    event: Option<&EventRef>,
    link: Option<&LinkRef>,
    attr: NestedAttrColumn<'_>,
    builder: &mut Vec<Vec<AttrValue>>,
) {
    let attributes = match attr.scope {
        NestedAttrScope::Event => event.map(|event| &event.attributes),
        NestedAttrScope::Link => link.map(|link| &link.attributes),
    };
    builder.push(
        attributes
            .into_iter()
            .flatten()
            .filter(|attribute| attribute.key == attr.key)
            .map(|attribute| attribute.value.clone())
            .collect(),
    );
}

pub(crate) fn append_nested_event(
    event: Option<&EventRef>,
    event_name: &mut StringBuilder,
    event_time_since_start: &mut Int64Builder,
) {
    if let Some(event) = event {
        event_name.append_value(&event.name);
        event_time_since_start.append_value(event.time_since_start.nanos_i64());
    } else {
        event_name.append_null();
        event_time_since_start.append_null();
    }
}

pub(crate) fn append_nested_link(
    link: Option<&LinkRef>,
    link_trace_id: &mut FixedSizeBinaryBuilder,
    link_span_id: &mut FixedSizeBinaryBuilder,
) -> Result<(), TraceqlError> {
    if let Some(link) = link {
        link_trace_id
            .append_value(link.trace_id)
            .map_err(|err| TraceqlError::Store(err.to_string()))?;
        link_span_id
            .append_value(link.span_id)
            .map_err(|err| TraceqlError::Store(err.to_string()))?;
    } else {
        link_trace_id.append_null();
        link_span_id.append_null();
    }
    Ok(())
}

pub(crate) fn matching_events_for_scan(
    batch: &RecordBatch,
    row: usize,
    matchers: &[SpanMatcher],
) -> Result<Vec<Option<EventRef>>, TraceqlError> {
    let event_matchers = matchers
        .iter()
        .filter(|matcher| is_event_matcher(matcher))
        .collect::<Vec<_>>();
    let events = event_values(batch, row)?;
    if event_matchers.is_empty() {
        return Ok(vec![events.into_iter().next()]);
    }
    if events.is_empty() {
        return Ok(vec![None]);
    }
    Ok(events
        .into_iter()
        .filter(|event| {
            event_matchers
                .iter()
                .all(|matcher| event_matcher_matches_event(event, matcher))
        })
        .take(1)
        .map(Some)
        .collect())
}

pub(crate) fn matching_links_for_scan(
    batch: &RecordBatch,
    row: usize,
    matchers: &[SpanMatcher],
) -> Result<Vec<Option<LinkRef>>, TraceqlError> {
    let link_matchers = matchers
        .iter()
        .filter(|matcher| is_link_matcher(matcher))
        .collect::<Vec<_>>();
    let links = link_values(batch, row)?;
    if link_matchers.is_empty() {
        return Ok(vec![links.into_iter().next()]);
    }
    if links.is_empty() {
        return Ok(vec![None]);
    }
    Ok(links
        .into_iter()
        .filter(|link| {
            link_matchers
                .iter()
                .all(|matcher| link_matcher_matches_link(link, matcher))
        })
        .take(1)
        .map(Some)
        .collect())
}

pub(crate) fn nested_attr_columns(matchers: &[SpanMatcher]) -> Vec<NestedAttrColumnProjection<'_>> {
    struct NestedAttrSelector {
        scope: NestedAttrScope,
        prefix: &'static str,
    }
    let mut out: Vec<NestedAttrColumnProjection<'_>> = Vec::new();
    for matcher in matchers {
        let selector = match matcher.scope {
            MatchScope::Event => NestedAttrSelector {
                scope: NestedAttrScope::Event,
                prefix: EVENT_ATTR_PREFIX,
            },
            MatchScope::Link => NestedAttrSelector {
                scope: NestedAttrScope::Link,
                prefix: LINK_ATTR_PREFIX,
            },
            MatchScope::Both
            | MatchScope::Span
            | MatchScope::Resource
            | MatchScope::Parent
            | MatchScope::Instrumentation
            | MatchScope::Intrinsic => continue,
        };
        let column = format!("{ATTR_PREFIX}{}{}", selector.prefix, matcher.key);
        if out.iter().any(|existing| existing.column == column) {
            continue;
        }
        out.push(NestedAttrColumnProjection {
            column,
            attribute: NestedAttrColumn {
                scope: selector.scope,
                key: &matcher.key,
            },
        });
    }
    out
}

#[derive(Clone, Copy)]
pub(crate) enum NestedAttrScope {
    Event,
    Link,
}

pub(crate) fn nested_event_matchers_match(
    batch: &RecordBatch,
    row: usize,
    matchers: &[SpanMatcher],
) -> Result<bool, TraceqlError> {
    let event_matchers = matchers
        .iter()
        .filter(|matcher| is_event_matcher(matcher))
        .collect::<Vec<_>>();
    if event_matchers.is_empty() {
        return Ok(true);
    }
    let events = event_values(batch, row)?;
    if events.is_empty() {
        return Ok(event_matchers
            .iter()
            .all(|matcher| event_matcher_matches_absence(matcher)));
    }
    Ok(events.iter().any(|event| {
        event_matchers
            .iter()
            .all(|matcher| event_matcher_matches_event(event, matcher))
    }))
}

pub(crate) fn nested_link_matchers_match(
    batch: &RecordBatch,
    row: usize,
    matchers: &[SpanMatcher],
) -> Result<bool, TraceqlError> {
    let link_matchers = matchers
        .iter()
        .filter(|matcher| is_link_matcher(matcher))
        .collect::<Vec<_>>();
    if link_matchers.is_empty() {
        return Ok(true);
    }
    let links = link_values(batch, row)?;
    if links.is_empty() {
        return Ok(link_matchers
            .iter()
            .all(|matcher| link_matcher_matches_absence(matcher)));
    }
    Ok(links.iter().any(|link| {
        link_matchers
            .iter()
            .all(|matcher| link_matcher_matches_link(link, matcher))
    }))
}

pub(crate) fn nested_string_attrs(
    keys: &ListArray,
    values: &ListArray,
    row: usize,
) -> Result<Vec<Attribute>, TraceqlError> {
    if keys.is_null(row) || values.is_null(row) {
        return Ok(Vec::new());
    }
    let key_values = keys.value(row);
    let key_values = krabka_traceql::string_array_from_arrow(key_values.as_ref(), || {
        TraceqlError::Store("nested attribute keys are not strings".into())
    })?;
    let value_lists = values.value(row);
    let value_lists = value_lists
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            TraceqlError::Store("nested attribute values are not string lists".into())
        })?;

    let mut out = Vec::new();
    for idx in 0..key_values.len().min(value_lists.len()) {
        if key_values.is_null(idx) || value_lists.is_null(idx) {
            continue;
        }
        let scalar_values = value_lists.value(idx);
        let scalar_values = scalar_values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                TraceqlError::Store("nested attribute scalar values are not strings".into())
            })?;
        if scalar_values.is_empty() || scalar_values.is_null(0) {
            continue;
        }
        out.push(Attribute::new(
            key_values.value(idx),
            AttrValue::Str(scalar_values.value(0).to_string()),
        ));
    }
    Ok(out)
}

pub(crate) fn recompute_batch_nested_sets(
    batch: &RecordBatch,
) -> Result<RecordBatch, TraceqlError> {
    let BatchNestedSets {
        left,
        right,
        parent_id,
        children,
    } = batch_nested_sets(SpanIdColumns {
        trace: fixed(batch, COL_TRACE_ID)?,
        span: fixed(batch, COL_SPAN_ID)?,
        parent_span: fixed(batch, COL_PARENT_SPAN_ID)?,
    });
    // childCount is PER TRACE: each parent's direct children, scoped to this
    // trace's rows. The nested-set `left` values reset to 1 per trace, so a
    // batch-global count would collide across traces and over-count.
    let mut child_count = vec![0_i32; batch.num_rows()];
    for entry in &children {
        let parent_row = entry.0;
        let kids = entry.1;
        child_count[*parent_row] = i32::try_from(kids.len()).unwrap_or(i32::MAX);
    }

    replace_scan_int32_columns(
        batch,
        &[
            ScanInt32ColumnReplacement {
                name: COL_NS_LEFT,
                values: left,
            },
            ScanInt32ColumnReplacement {
                name: COL_NS_RIGHT,
                values: right,
            },
            ScanInt32ColumnReplacement {
                name: COL_PARENT_ID,
                values: parent_id,
            },
            ScanInt32ColumnReplacement {
                name: COL_CHILD_COUNT,
                values: child_count,
            },
        ],
    )
}

pub(crate) fn recompute_scan_nested_sets(
    batches: Vec<RecordBatch>,
    max_scan_concat: ByteSize,
) -> Result<Vec<RecordBatch>, TraceqlError> {
    // `concat_batches` materialises every matched span into one RecordBatch.
    // Arrow's variable-length columns use i32 offsets, so a combined batch over
    // ~2 GiB overflows with an opaque `Offset overflow error`. Cap the merge at a
    // safe 1.5 GiB and surface an actionable error so a pathological query (an
    // unbounded time range over a huge tenant) degrades cleanly instead of
    // emitting `concat scan batches: Offset overflow error`.
    if batches.is_empty() {
        return Ok(batches);
    }
    let schema = batches[0].schema();
    let batches = align_scan_batches_to_schema(batches, &schema)?;
    let total: ByteSize = batches
        .iter()
        .map(|batch| {
            ByteSize::from_bytes(u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX))
        })
        .sum();
    if total > max_scan_concat {
        return Err(TraceqlError::Store(format!(
            "scan result too large to merge ({} bytes > {} cap); \
             narrow the query time range or selector",
            total.bytes_usize(),
            max_scan_concat.bytes_usize()
        )));
    }
    let batch = concat_batches(&schema, &batches)
        .map_err(|err| TraceqlError::Store(format!("concat scan batches: {err}")))?;
    recompute_batch_nested_sets(&batch).map(|batch| vec![batch])
}

pub(crate) fn recompute_trace_nested_sets(spans: &mut [SpanRef]) {
    let nodes = spans
        .iter()
        .map(|span| SpanNode {
            span_id: span.span_id,
            parent_span_id: span.parent_span_id,
        })
        .collect::<Vec<_>>();
    for entry in spans
        .iter_mut()
        .zip(assign_nested_set(&nodes, CycleSpans::LeaveUnassigned))
    {
        let span = entry.0;
        let nested_set = entry.1;
        span.nested_set_left = nested_set.nested_set_left;
        span.nested_set_right = nested_set.nested_set_right;
        span.nested_set_parent = nested_set.parent_id;
    }
}
