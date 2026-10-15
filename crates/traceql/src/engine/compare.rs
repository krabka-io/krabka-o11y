use super::{
    Array, AsArray, AttrValue, Attribute, BTreeMap, COL_DURATION, COL_KIND, COL_NAME,
    COL_ROOT_SERVICE_NAME, COL_SPAN_ID, COL_STATUS_CODE, COL_STATUS_MESSAGE, COL_TRACE_ID,
    ComparisonOp, DataType, EVENT_ATTR_PREFIX, Field, FieldExpr, HashMap, HashSet, Intrinsic,
    LINK_ATTR_PREFIX, MetricPoint, MetricsRange, RESOURCE_ATTR_PREFIX, RecordBatch, Result, Scope,
    SpansetExpr, TraceMetricSeries, TraceqlError, UnixNano, Value, block_row_scoped_attrs,
    bool_cmp, collect_selection_regexes, compare_row_in_selection_window, f64_from_u64, fixed_8,
    fixed_16, float_cmp, i32_value, i64_value, kind_enum_name, kind_enum_value, num_cmp,
    push_scoped_attr, row_attrs, span_start_column, status_enum_name, status_enum_value,
    string_value,
};

// Compare Attr Values Match
/// Applies Tempo array-attribute semantics to one attribute.
///
/// Equality, `<`, `>`, and regex match if ANY value satisfies the predicate.
/// `!=` and `!~` match only if ALL values satisfy the predicate. An absent
/// attribute matches ONLY `= nil`. This mirrors the planner SQL, where
/// `NULL != v` excludes the row, and the nil rules of the in-memory store. A
/// `!= <concrete>` or `!~` predicate does NOT pull a span without the
/// attribute into the selection.
pub(crate) fn compare_attr_values_match(
    values: &[&AttrValue],
    op: ComparisonOp,
    rhs: &Value,
    regexes: &CompareRegexCache,
) -> bool {
    if values.is_empty() {
        return matches!((op, rhs), (ComparisonOp::Eq, Value::Nil));
    }
    if matches!(rhs, Value::Nil) {
        // present value: `= nil` is false, `!= nil` is true.
        return matches!(op, ComparisonOp::Neq);
    }
    match op {
        ComparisonOp::Neq | ComparisonOp::Nre => values
            .iter()
            .all(|value| compare_value_match(value, op, rhs, regexes)),
        _ => values
            .iter()
            .any(|value| compare_value_match(value, op, rhs, regexes)),
    }
}

// Compare By Attr
/// `attribute → value → group → per-bucket counts`.
///
/// `build_compare_series` regroups the flat counts into this shape. The shape
/// lets the code choose one shared value set per attribute across both groups.
/// Grafana shows the baseline distribution and the selection distribution side
/// by side, so both must cover the same values.
pub(crate) type CompareByAttr =
    BTreeMap<String, BTreeMap<String, BTreeMap<CompareGroup, Vec<u64>>>>;

// Compare Comparison Matches
pub(crate) fn compare_comparison_matches(
    field: &Field,
    op: ComparisonOp,
    rhs: &Value,
    row: &CompareRow,
    regexes: &CompareRegexCache,
) -> bool {
    match compare_field_class(field) {
        Ok(CompareFieldClass::Attr { scope, key }) => {
            let values = compare_row_attr_values(row, &scope, &key);
            compare_attr_values_match(&values, op, rhs, regexes)
        }
        Ok(CompareFieldClass::Intrinsic(intrinsic)) => {
            compare_intrinsic_matches(row, &intrinsic, op, rhs, regexes)
        }
        Err(_) => false,
    }
}

// Compare Counts
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CompareCountKey {
    pub(crate) group: CompareGroup,
    pub(crate) attr_key: String,
    pub(crate) value: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CompareAttributeKey {
    pub(crate) group: CompareGroup,
    pub(crate) attr_key: String,
}

/// Upper bound on the distinct values tracked per `(group, attribute)`.
///
/// The bound applies during accumulation, before the `top_n` cut. Without the
/// bound, a high-cardinality attribute such as a URL, a request id, or a UUID
/// on every span inserts one bucket-length `Vec` per distinct value and
/// exhausts memory. When a `(group, attribute)` reaches this cap, the code
/// drops new distinct values, and the values it already tracks keep counting.
/// Memory then stays at `O(attrs * cap * buckets)`. The code clamps the cap to
/// at least `top_n`, so the final `top_n` cut in `build_compare_series` is
/// never starved.
pub(crate) type CompareCounts = BTreeMap<CompareCountKey, Vec<u64>>;

// Compare Field Class
/// A span attribute, a resource attribute, or a selection-evaluable intrinsic.
///
/// The compare code classifies a field into this enum for per-row lookup. The
/// classification rejects parent references, which pinned Tempo does not support.
pub(crate) enum CompareFieldClass {
    /// A span-scoped or resource-scoped attribute, keyed by its raw key.
    /// `Both` matches either scope. `Span` and `Resource` pin the scope.
    Attr { scope: Scope, key: String },
    /// A selection-evaluable intrinsic with its row value.
    Intrinsic(Intrinsic),
}

pub(crate) fn compare_field_class(field: &Field) -> Result<CompareFieldClass> {
    match &field.scope {
        Scope::Both
        | Scope::Span
        | Scope::Resource
        | Scope::Event
        | Scope::Link
        | Scope::Instrumentation => Ok(CompareFieldClass::Attr {
            scope: field.scope.clone(),
            key: field.key.clone(),
        }),
        Scope::Intrinsic(intrinsic) => Ok(CompareFieldClass::Intrinsic(intrinsic.clone())),
        other @ Scope::Parent => Err(TraceqlError::Unsupported(format!(
            "compare() selection does not support {other:?}"
        ))),
    }
}

// Compare Field Present
pub(crate) fn compare_field_present(field: &Field, row: &CompareRow) -> bool {
    match compare_field_class(field) {
        Ok(CompareFieldClass::Attr { scope, key }) => {
            !compare_row_attr_values(row, &scope, &key).is_empty()
        }
        Ok(CompareFieldClass::Intrinsic(intrinsic)) => compare_intrinsic_present(row, &intrinsic),
        Err(_) => false,
    }
}

// Compare Group
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CompareGroup {
    Baseline,
    Selection,
}

impl CompareGroup {
    pub(crate) fn meta_type(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::Selection => "selection",
        }
    }

    pub(crate) fn total_meta_type(self) -> &'static str {
        match self {
            Self::Baseline => "baseline_total",
            Self::Selection => "selection_total",
        }
    }
}

// Compare Group For Row
/// Determines the compare group of a span row.
///
/// The group is `Selection` when the row matches the compare selection spanset
/// and falls inside the optional `[start, end]` sub-window. In every other
/// case the group is `Baseline`.
pub(crate) fn compare_group_for_row(
    row: &CompareRow,
    compare: &CompareSpec,
    regexes: &CompareRegexCache,
    selected_by_plan: Option<bool>,
) -> CompareGroup {
    if compare_row_in_selection_window(row, compare)
        && selected_by_plan.unwrap_or_else(|| spanset_matches_row(&compare.selection, row, regexes))
    {
        CompareGroup::Selection
    } else {
        CompareGroup::Baseline
    }
}

// Compare Intrinsic Matches
pub(crate) fn compare_intrinsic_matches(
    row: &CompareRow,
    intrinsic: &Intrinsic,
    op: ComparisonOp,
    rhs: &Value,
    regexes: &CompareRegexCache,
) -> bool {
    let field_values_match = || {
        super::field_comparison::field_values(
            &super::Field {
                scope: super::Scope::Intrinsic(intrinsic.clone()),
                key: String::new(),
            },
            row,
        )
        .iter()
        .any(|value| super::field_comparison::scalar_matches(value, op, rhs))
    };
    match intrinsic {
        Intrinsic::Name => row.name.as_ref().is_some_and(
            |name| matches!(rhs, Value::Str(rhs) if string_cmp(name, op, rhs, regexes)),
        ),
        Intrinsic::StatusMessage => row
            .status_message
            .as_ref()
            .is_some_and(|msg| matches!(rhs, Value::Str(rhs) if string_cmp(msg, op, rhs, regexes))),
        Intrinsic::Status => row
            .status_code
            .is_some_and(|code| enum_cmp(code, op, rhs, status_enum_value)),
        Intrinsic::Kind => row
            .kind
            .is_some_and(|code| enum_cmp(code, op, rhs, kind_enum_value)),
        Intrinsic::Duration => row.duration.is_some_and(|duration| match rhs {
            Value::Int(rhs) | Value::Duration(rhs) => num_cmp(duration, op, *rhs),
            _ => field_values_match(),
        }),
        _ => field_values_match(),
    }
}

// Compare Intrinsic Present
pub(crate) fn compare_intrinsic_present(row: &CompareRow, intrinsic: &Intrinsic) -> bool {
    match intrinsic {
        Intrinsic::Name => row.name.as_ref().is_some_and(|name| !name.is_empty()),
        Intrinsic::Status => row.status_code.is_some(),
        Intrinsic::StatusMessage => row
            .status_message
            .as_ref()
            .is_some_and(|msg| !msg.is_empty()),
        Intrinsic::Kind => row.kind.is_some(),
        Intrinsic::Duration => row.duration.is_some(),
        _ => !super::field_comparison::field_values(
            &super::Field {
                scope: super::Scope::Intrinsic(intrinsic.clone()),
                key: String::new(),
            },
            row,
        )
        .is_empty(),
    }
}

// Compare Points
pub(crate) fn compare_points(buckets: &[u64], range: MetricsRange) -> Vec<MetricPoint> {
    buckets
        .iter()
        .enumerate()
        .map(|indexed_count| {
            let idx = indexed_count.0;
            let count = indexed_count.1;
            let ts = range.output_start.0 + i64::try_from(idx).unwrap_or(i64::MAX) * range.step.0;
            MetricPoint::new(ts, f64_from_u64(*count).unwrap_or(0.0))
        })
        .collect()
}

// Compare Regex Cache
/// Per-query cache of compiled selection regexes, keyed by the raw pattern.
///
/// The key is the raw `=~`/`!~` pattern string. Each value is the pattern
/// compiled as `^(?:pat)$`, which matches how `string_cmp` anchors a pattern.
/// `assemble_compare_response` builds the cache once, and every scanned row
/// reuses it, so a regex selection does not recompile per span. A pattern that
/// does not compile stays absent from the map, and the lookup treats an absent
/// pattern as a non-match. This keeps the earlier best-effort behavior instead
/// of a failed query.
pub(crate) type CompareRegexCache = HashMap<String, regex::Regex>;

// Compare Row
/// One scanned span row, projected into the values the compare needs.
///
/// The values are the span start time, every scoped attribute, and the
/// selection-evaluable intrinsics. The compare uses the attributes for the
/// distribution and the intrinsics for group membership.
pub(crate) struct CompareRow {
    pub(crate) ts: UnixNano,
    /// Fully-scoped attribute key → display value, deduplicated. Example keys:
    /// `span.http.method` and `resource.service.name`.
    pub(crate) attrs: Vec<DisplayAttribute>,
    /// Raw span or resource attribute key → typed values, with repeated
    /// attributes allowed. The compare uses these values to evaluate the
    /// selection's attribute comparisons.
    pub(crate) raw_span_attrs: Vec<Attribute>,
    pub(crate) raw_resource_attrs: Vec<Attribute>,
    pub(crate) name: Option<String>,
    pub(crate) status_code: Option<i32>,
    pub(crate) status_message: Option<String>,
    pub(crate) kind: Option<i32>,
    pub(crate) duration: Option<i64>,
    pub(crate) columns: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DisplayAttribute {
    pub(crate) key: String,
    pub(crate) value: String,
}

impl DisplayAttribute {
    pub(crate) fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
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
    let mut attrs: Vec<DisplayAttribute> = Vec::new();
    let mut raw_span_attrs: Vec<Attribute> = Vec::new();
    let mut raw_resource_attrs: Vec<Attribute> = Vec::new();

    // Promoted `attr.<key>` columns are span-scoped attributes. Event/link
    // attrs stay available to the typed evaluator, but their child scope must
    // not enter the per-span distribution as span attributes.
    let packed_attrs = block_row_scoped_attrs(batch, row)?;
    for attribute in row_attrs(batch, row)? {
        let key = attribute.key;
        let value = attribute.value;
        if packed_attrs
            .iter()
            .any(|packed_attribute| packed_attribute.key == key)
        {
            continue;
        }
        if !key.starts_with(EVENT_ATTR_PREFIX) && !key.starts_with(LINK_ATTR_PREFIX) {
            push_scoped_attr(&mut attrs, "span", &key, &value);
        }
        raw_span_attrs.push(Attribute::new(key, value));
    }
    // Block attribute-list columns carry the remaining span + resource attrs.
    for attribute in packed_attrs {
        let key = attribute.key;
        let value = attribute.value;
        if let Some(stripped) = key.strip_prefix(RESOURCE_ATTR_PREFIX) {
            // The per-span `__resource.service.name` block attr would
            // double-count `resource.service.name`, which the rest of the
            // engine defines as the TRACE-ROOT service (COL_ROOT_SERVICE_NAME,
            // emitted below). Skip it so the root column is the sole emitter.
            if stripped == "service.name" {
                continue;
            }
            push_scoped_attr(&mut attrs, "resource", stripped, &value);
            raw_resource_attrs.push(Attribute::new(stripped.to_string(), value));
        } else {
            if !key.starts_with(EVENT_ATTR_PREFIX) && !key.starts_with(LINK_ATTR_PREFIX) {
                push_scoped_attr(&mut attrs, "span", &key, &value);
            }
            raw_span_attrs.push(Attribute::new(key, value));
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
        raw_resource_attrs.push(Attribute::new("service.name", AttrValue::Str(service)));
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
        attrs.push(DisplayAttribute::new("name", name.clone()));
    }
    if let Some(code) = status_code {
        attrs.push(DisplayAttribute::new("status", status_enum_name(code)));
    }
    if let Some(code) = kind {
        attrs.push(DisplayAttribute::new("kind", kind_enum_name(code)));
    }

    let mut columns = BTreeMap::new();
    for indexed_field in batch.schema().fields().iter().enumerate() {
        let index = indexed_field.0;
        let field = indexed_field.1;
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

// Compare Row Attr Values
use crate::result::normalized_attr_value;

pub(crate) fn compare_row_attr_values<'a>(
    row: &'a CompareRow,
    scope: &Scope,
    key: &str,
) -> Vec<&'a AttrValue> {
    let prefixed;
    let key = match scope {
        Scope::Event => {
            prefixed = format!("{}{key}", super::EVENT_ATTR_PREFIX);
            &prefixed
        }
        Scope::Link => {
            prefixed = format!("{}{key}", super::LINK_ATTR_PREFIX);
            &prefixed
        }
        Scope::Instrumentation => {
            prefixed = format!("{}{key}", super::INSTRUMENTATION_ATTR_PREFIX);
            &prefixed
        }
        _ => key,
    };
    let mut out = Vec::new();
    let want_span = matches!(
        scope,
        Scope::Both | Scope::Span | Scope::Event | Scope::Link | Scope::Instrumentation
    );
    let want_resource = matches!(scope, Scope::Both | Scope::Resource);
    if want_span {
        out.extend(
            row.raw_span_attrs
                .iter()
                .filter(|attribute| attribute.key == key)
                .map(|attribute| &attribute.value),
        );
    }
    // Tempo vParquet AttributeFor checks span attributes before falling back
    // to resource attributes for an unscoped key.
    if want_resource && (!matches!(scope, Scope::Both) || out.is_empty()) {
        out.extend(
            row.raw_resource_attrs
                .iter()
                .filter(|attribute| attribute.key == key)
                .map(|attribute| &attribute.value),
        );
    }
    if matches!(scope, Scope::Both) && out.is_empty() {
        for prefix in [
            super::EVENT_ATTR_PREFIX,
            super::LINK_ATTR_PREFIX,
            super::INSTRUMENTATION_ATTR_PREFIX,
        ] {
            let prefixed = format!("{prefix}{key}");
            out.extend(
                row.raw_span_attrs
                    .iter()
                    .filter(|attribute| attribute.key == prefixed)
                    .map(|attribute| &attribute.value),
            );
            if !out.is_empty() {
                break;
            }
        }
    }
    // Pinned vParquet5 attributeCollector ignores IsArray: empty lists are
    // nil and a single element is scalar. Keep original shape for retrieval.
    out.into_iter().filter_map(normalized_attr_value).collect()
}

// Compare Span Identities
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SpanIdentity {
    pub(crate) trace_id: [u8; 16],
    pub(crate) span_id: [u8; 8],
}

pub(crate) fn compare_span_identities(batches: &[RecordBatch]) -> Result<HashSet<SpanIdentity>> {
    let mut identities = HashSet::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            identities.insert(SpanIdentity {
                trace_id: fixed_16(batch, COL_TRACE_ID, row)?,
                span_id: fixed_8(batch, COL_SPAN_ID, row)?,
            });
        }
    }
    Ok(identities)
}

// Compare Spec
/// The parsed `compare({selection}, topN [, start, end])` spec on a `MetricPlan`.
///
/// Execution scans the outer spanset and splits its spans into two groups. The
/// `selection` group holds the spans that also match `selection`. The
/// `baseline` group holds the rest. Execution then emits per-attribute
/// value-distribution series.
#[derive(Clone)]
pub(crate) struct CompareSpec {
    pub(crate) selection: crate::ast::SpansetExpr,
    pub(crate) top_n: usize,
    pub(crate) start: Option<UnixNano>,
    pub(crate) end: Option<UnixNano>,
}

// Compare Total Series
pub(crate) fn compare_total_series(
    group: CompareGroup,
    buckets: &[u64],
    range: MetricsRange,
) -> TraceMetricSeries {
    TraceMetricSeries {
        labels: vec![super::MetricLabel::new(
            META_TYPE_KEY,
            group.total_meta_type(),
        )],
        label_types: BTreeMap::default(),
        points: compare_points(buckets, range),
        exemplars: Vec::new(),
    }
}

// Compare Totals
pub(crate) type CompareTotals = BTreeMap<CompareGroup, Vec<u64>>;

// Compare Value Match
struct ComparedValues<'a> {
    actual: &'a AttrValue,
    expected: &'a Value,
}

pub(crate) fn compare_value_match(
    value: &AttrValue,
    op: ComparisonOp,
    rhs: &Value,
    regexes: &CompareRegexCache,
) -> bool {
    match (ComparedValues {
        actual: value,
        expected: rhs,
    }) {
        ComparedValues {
            actual: AttrValue::Array(values),
            expected: rhs,
        } => {
            if matches!(op, ComparisonOp::Neq | ComparisonOp::Nre) {
                values
                    .iter()
                    .all(|value| compare_value_match(value, op, rhs, regexes))
            } else {
                values
                    .iter()
                    .any(|value| compare_value_match(value, op, rhs, regexes))
            }
        }
        ComparedValues {
            actual: AttrValue::Str(value),
            expected: Value::Str(rhs),
        } => string_cmp(value, op, rhs, regexes),
        ComparedValues {
            actual: AttrValue::Int(value),
            expected: Value::Int(rhs) | Value::Duration(rhs),
        } => num_cmp(*value, op, *rhs),
        ComparedValues {
            actual: AttrValue::Float(value),
            expected: Value::Float(rhs),
        } => float_cmp(*value, op, *rhs),
        ComparedValues {
            actual: AttrValue::Bool(value),
            expected: Value::Bool(rhs),
        } => bool_cmp(*value, op, *rhs),
        _ => false,
    }
}

// Compare Value Series
pub(crate) fn compare_value_series(
    group: CompareGroup,
    attr_key: &str,
    value: &str,
    buckets: &[u64],
    range: MetricsRange,
) -> TraceMetricSeries {
    TraceMetricSeries {
        labels: vec![
            super::MetricLabel::new(META_TYPE_KEY, group.meta_type()),
            super::MetricLabel::new(attr_key, value),
        ],
        label_types: BTreeMap::default(),
        points: compare_points(buckets, range),
        exemplars: Vec::new(),
    }
}

#[cfg(test)]
mod compare_row_tests {
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

#[cfg(test)]
mod compare_row_attr_values_tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn unscoped_values_prefer_span_and_fall_back_only_when_absent() {
        let row = CompareRow {
            ts: super::super::UnixNano(0),
            attrs: Vec::new(),
            raw_span_attrs: vec![crate::result::Attribute::new("shared", AttrValue::Int(1))],
            raw_resource_attrs: vec![
                crate::result::Attribute::new("shared", AttrValue::Str("1".into())),
                crate::result::Attribute::new("fallback", AttrValue::Bool(true)),
            ],
            name: None,
            status_code: None,
            status_message: None,
            kind: None,
            duration: None,
            columns: std::collections::BTreeMap::new(),
        };
        assert!(compare_row_attr_values(&row, &Scope::Both, "shared") == vec![&AttrValue::Int(1)]);
        assert!(
            compare_row_attr_values(&row, &Scope::Resource, "shared")
                == vec![&AttrValue::Str("1".into())]
        );
        assert!(
            compare_row_attr_values(&row, &Scope::Both, "fallback") == vec![&AttrValue::Bool(true)]
        );
    }
}

pub(crate) fn enum_cmp(
    value: i32,
    op: ComparisonOp,
    rhs: &Value,
    enum_value: fn(&str) -> Option<i32>,
) -> bool {
    let expected = match rhs {
        Value::Str(name) => enum_value(&name.to_ascii_lowercase()),
        Value::Int(value) => i32::try_from(*value).ok(),
        _ => None,
    };
    expected.is_some_and(|expected| num_cmp(i64::from(value), op, i64::from(expected)))
}

pub(crate) fn string_cmp(
    value: &str,
    op: ComparisonOp,
    rhs: &str,
    regexes: &CompareRegexCache,
) -> bool {
    match op {
        ComparisonOp::Eq => value == rhs,
        ComparisonOp::Neq => value != rhs,
        // The pattern was precompiled once per query (see CompareRegexCache); an
        // uncompilable pattern is absent from the cache and yields non-match.
        ComparisonOp::Re => regexes.get(rhs).is_some_and(|re| re.is_match(value)),
        ComparisonOp::Nre => regexes.get(rhs).is_some_and(|re| !re.is_match(value)),
        ComparisonOp::Lt | ComparisonOp::Lte | ComparisonOp::Gt | ComparisonOp::Gte => false,
    }
}

pub(crate) fn validate_compare_field_expr(fe: &FieldExpr) -> Result<()> {
    match fe {
        FieldExpr::ExpressionComparison { lhs, rhs, .. } => {
            let mut fields = Vec::new();
            lhs.collect_fields(&mut fields);
            rhs.collect_fields(&mut fields);
            for field in fields {
                compare_field_class(field)?;
            }
            Ok(())
        }
        FieldExpr::FieldComparison { lhs, rhs, .. } => {
            compare_field_class(lhs)?;
            compare_field_class(rhs).map(|_| ())
        }
        FieldExpr::Comparison { lhs, .. } | FieldExpr::Field(lhs) => {
            compare_field_class(lhs).map(|_| ())
        }
        FieldExpr::And(a, b) | FieldExpr::Or(a, b) => {
            validate_compare_field_expr(a)?;
            validate_compare_field_expr(b)
        }
        FieldExpr::Not(inner) => validate_compare_field_expr(inner),
        FieldExpr::Const(_) => Ok(()),
    }
}

/// Validates that the per-row evaluator supports a compare selection spanset.
///
/// The evaluator supports a selector whose `FieldExpr` is a boolean
/// combination of span, resource, and intrinsic comparisons. It also supports
/// a spanset-level `And` or `Or` of such selectors. This function rejects
/// structural operators and the parent, event, and link scopes.
pub(crate) fn validate_compare_selection(selection: &SpansetExpr) -> Result<()> {
    fn needs_nested_projection(field: &super::Field) -> bool {
        matches!(
            field.scope,
            super::Scope::Event
                | super::Scope::Link
                | super::Scope::Intrinsic(
                    super::Intrinsic::EventName
                        | super::Intrinsic::EventTimeSinceStart
                        | super::Intrinsic::LinkTraceId
                        | super::Intrinsic::LinkSpanId
                )
        )
    }
    fn reject_arithmetic(expr: &super::FieldExpr) -> Result<()> {
        match expr {
            super::FieldExpr::Field(_) => Err(TraceqlError::Unsupported(
                "compare selection requires typed boolean evaluation".into(),
            )),
            super::FieldExpr::ExpressionComparison { .. } => Err(TraceqlError::Unsupported(
                "compare() selection scalar arithmetic".into(),
            )),
            super::FieldExpr::And(lhs, rhs) | super::FieldExpr::Or(lhs, rhs) => {
                reject_arithmetic(lhs)?;
                reject_arithmetic(rhs)
            }
            super::FieldExpr::Not(inner) => reject_arithmetic(inner),
            super::FieldExpr::Comparison { lhs, .. } if needs_nested_projection(lhs) => Err(
                TraceqlError::Unsupported("compare selection needs nested projection".into()),
            ),
            super::FieldExpr::FieldComparison { lhs, rhs, .. }
                if needs_nested_projection(lhs) || needs_nested_projection(rhs) =>
            {
                Err(TraceqlError::Unsupported(
                    "compare selection needs nested projection".into(),
                ))
            }
            _ => Ok(()),
        }
    }
    match selection {
        SpansetExpr::Selector(fe) => {
            reject_arithmetic(fe)?;
            validate_compare_field_expr(fe)
        }
        SpansetExpr::And(lhs, rhs) | SpansetExpr::Or(lhs, rhs) => {
            validate_compare_selection(lhs)?;
            validate_compare_selection(rhs)
        }
        SpansetExpr::Structural { .. } => Err(TraceqlError::Unsupported(
            "compare() selection does not support structural operators".into(),
        )),
    }
}

pub(crate) fn field_expr_matches_row(
    fe: &FieldExpr,
    row: &CompareRow,
    regexes: &CompareRegexCache,
) -> bool {
    match fe {
        FieldExpr::ExpressionComparison { lhs, op, rhs } => {
            super::scalar_expression_value(lhs, row)
                .ok()
                .zip(super::scalar_expression_value(rhs, row).ok())
                .is_some_and(|values| {
                    super::field_comparison::scalar_matches(&values.0, *op, &values.1)
                })
        }
        FieldExpr::Const(value) => *value,
        FieldExpr::And(a, b) => {
            field_expr_matches_row(a, row, regexes) && field_expr_matches_row(b, row, regexes)
        }
        FieldExpr::Or(a, b) => {
            field_expr_matches_row(a, row, regexes) || field_expr_matches_row(b, row, regexes)
        }
        FieldExpr::Not(inner) => !field_expr_matches_row(inner, row, regexes),
        FieldExpr::Field(field) if matches!(field.scope, Scope::Parent) => {
            compare_field_present(field, row)
        }
        FieldExpr::Field(field) => {
            // Tempo retains only a scalar boolean true; arrays and other types do not match.
            matches!(
                super::field_comparison::field_values(field, row).as_slice(),
                [Value::Bool(true)]
            )
        }
        FieldExpr::FieldComparison { lhs, op, rhs } => {
            super::field_comparison::field_comparison_matches(lhs, *op, rhs, row)
        }
        FieldExpr::Comparison { lhs, op, rhs } => {
            compare_comparison_matches(lhs, *op, rhs, row, regexes)
        }
    }
}

pub(crate) fn spanset_matches_row(
    selection: &SpansetExpr,
    row: &CompareRow,
    regexes: &CompareRegexCache,
) -> bool {
    match selection {
        SpansetExpr::Selector(fe) => field_expr_matches_row(fe, row, regexes),
        SpansetExpr::And(lhs, rhs) => {
            spanset_matches_row(lhs, row, regexes) && spanset_matches_row(rhs, row, regexes)
        }
        SpansetExpr::Or(lhs, rhs) => {
            spanset_matches_row(lhs, row, regexes) || spanset_matches_row(rhs, row, regexes)
        }
        // Rejected by validate_compare_selection; treat as non-match defensively.
        SpansetExpr::Structural { .. } => false,
    }
}

/// The reserved label key that Tempo uses to tag a compare series.
///
/// The tag holds the group, `baseline` or `selection`, or the per-group total,
/// `baseline_total` or `selection_total`. Grafana's exploretraces Comparison
/// view reads this key.
pub(crate) const META_TYPE_KEY: &str = "__meta_type";

/// Scans the batches and accumulates per-bucket span counts.
///
/// The counts are `counts[(group, attr_key, value)][bucket]` and
/// `totals[group][bucket]`. `COMPARE_MAX_VALUES_PER_ATTR` bounds the number of
/// DISTINCT values tracked per `(group, attr_key)`, clamped up to `top_n`.
/// When the count reaches that bound, this function drops new distinct values,
/// and the values it already tracks keep counting. Memory stays at
/// `O(attrs * cap * buckets)` for any attribute cardinality.
pub(crate) struct CompareAccumulation {
    pub(crate) counts: CompareCounts,
    pub(crate) totals: CompareTotals,
}

pub(crate) fn accumulate_compare_counts(
    batches: &[RecordBatch],
    compare: &CompareSpec,
    range: MetricsRange,
    bucket_count: usize,
    max_values_per_attr: usize,
    selected_spans: Option<&HashSet<SpanIdentity>>,
) -> Result<CompareAccumulation> {
    let mut counts: CompareCounts = BTreeMap::new();
    let mut totals: CompareTotals = BTreeMap::new();
    // Distinct values already tracked per (group, attr_key), to enforce
    // the configured per-attribute cap during accumulation.
    let mut distinct_per_attr: BTreeMap<CompareAttributeKey, usize> = BTreeMap::new();
    // The cap must never fall below top_n or the final cut would be starved.
    let value_cap = max_values_per_attr.max(compare.top_n);
    // Compile every selection regex once, up front, and reuse across all rows.
    let mut regexes: CompareRegexCache = HashMap::new();
    collect_selection_regexes(&compare.selection, &mut regexes);

    for batch in batches {
        let starts = span_start_column(batch)?;
        for row in 0..batch.num_rows() {
            let ts = UnixNano(starts.value(row));
            if ts < range.scan_start || ts > range.scan_end {
                continue;
            }
            let bucket = if range.instant {
                0
            } else {
                usize::try_from((ts.0 - range.scan_start.0) / range.step.0)
                    .map_err(|e| TraceqlError::Exec(e.to_string()))?
            };
            let compare_row = compare_row(batch, row, ts)?;
            let selected_by_plan = if let Some(selected) = selected_spans {
                Some(selected.contains(&SpanIdentity {
                    trace_id: fixed_16(batch, COL_TRACE_ID, row)?,
                    span_id: fixed_8(batch, COL_SPAN_ID, row)?,
                }))
            } else {
                None
            };
            let group = compare_group_for_row(&compare_row, compare, &regexes, selected_by_plan);
            let group_totals = totals.entry(group).or_insert_with(|| vec![0; bucket_count]);
            if let Some(slot) = group_totals.get_mut(bucket) {
                *slot += 1;
            }
            for attribute in &compare_row.attrs {
                let key = CompareCountKey {
                    group,
                    attr_key: attribute.key.clone(),
                    value: attribute.value.clone(),
                };
                // An already-tracked value keeps counting regardless of cap; a
                // new distinct value is only tracked while under the per-attr cap.
                if !counts.contains_key(&key) {
                    let distinct = distinct_per_attr
                        .entry(CompareAttributeKey {
                            group,
                            attr_key: attribute.key.clone(),
                        })
                        .or_insert(0);
                    if *distinct >= value_cap {
                        continue;
                    }
                    *distinct += 1;
                }
                let series = counts.entry(key).or_insert_with(|| vec![0; bucket_count]);
                if let Some(slot) = series.get_mut(bucket) {
                    *slot += 1;
                }
            }
        }
    }
    Ok(CompareAccumulation { counts, totals })
}

struct RankedCompareValue<'a> {
    value: &'a String,
    selection_total: u64,
}

/// Builds the compare time series from the accumulated per-bucket counts.
///
/// For each attribute, this function picks ONE shared value set: the `top_n`
/// values with the highest SELECTION-group count. It breaks ties by value in
/// ascending order, which makes the result deterministic. The selection is the
/// group that the user investigates.
///
/// The function emits BOTH a baseline series and a selection series for each
/// of those values, with the label
/// `{__meta_type=<group>, <attribute>=<value>}`. It zero-fills a value that is
/// absent from a group, so the baseline distribution and the selection
/// distribution cover the same values. Grafana's Comparison view shows them
/// side by side. The function emits per-group totals as
/// `{__meta_type=<group>_total}`.
pub(crate) fn build_compare_series(
    counts: CompareCounts,
    totals: &CompareTotals,
    top_n: usize,
    range: MetricsRange,
    bucket_count: usize,
) -> Vec<TraceMetricSeries> {
    // Regroup the flat (group, attr_key, value) entries into attr → value →
    // group → buckets so a single shared value set per attribute can be picked.
    let mut by_attr: CompareByAttr = BTreeMap::new();
    for count_entry in counts {
        let key = count_entry.0;
        let buckets = count_entry.1;
        by_attr
            .entry(key.attr_key)
            .or_default()
            .entry(key.value)
            .or_default()
            .insert(key.group, buckets);
    }

    let mut series = Vec::new();
    for attr_entry in by_attr {
        let attr_key = attr_entry.0;
        let values = attr_entry.1;
        // Rank values by the SELECTION-group count (descending), tie-broken by
        // value ascending. This yields a single, deterministic top_n value set
        // shared by both groups.
        let mut ranked: Vec<RankedCompareValue<'_>> = values
            .iter()
            .map(|value_entry| {
                let value = value_entry.0;
                let per_group = value_entry.1;
                let selection_total: u64 = per_group
                    .get(&CompareGroup::Selection)
                    .map_or(0, |buckets| buckets.iter().sum());
                RankedCompareValue {
                    value,
                    selection_total,
                }
            })
            .collect();
        ranked.sort_by(|a, b| {
            b.selection_total
                .cmp(&a.selection_total)
                .then_with(|| a.value.cmp(b.value))
        });
        ranked.truncate(top_n);

        // Emit a baseline and a selection series for each chosen value; a value
        // missing from a group is zero-filled so both groups share the value set.
        for ranked_value in ranked {
            let per_group = &values[ranked_value.value];
            for group in [CompareGroup::Baseline, CompareGroup::Selection] {
                let buckets = per_group
                    .get(&group)
                    .cloned()
                    .unwrap_or_else(|| vec![0; bucket_count]);
                series.push(compare_value_series(
                    group,
                    &attr_key,
                    ranked_value.value,
                    &buckets,
                    range,
                ));
            }
        }
    }

    // Per-group totals, including a zero-valued total for any group with no
    // spans so Grafana always has a denominator for both groups.
    for group in [CompareGroup::Baseline, CompareGroup::Selection] {
        let buckets = totals
            .get(&group)
            .cloned()
            .unwrap_or_else(|| vec![0; bucket_count]);
        series.push(compare_total_series(group, &buckets, range));
    }

    series
}
