use krabka_units::convert::TimeExt as _;

use super::{
    ATTR_PREFIX, Aggregate, Array, AsArray, AttrValue, BTreeMap, COL_CHILD_COUNT, COL_DURATION,
    COL_EVENT_NAME, COL_EVENT_TIME_SINCE_START, COL_INSTRUMENTATION_NAME,
    COL_INSTRUMENTATION_VERSION, COL_KIND, COL_LINK_SPAN_ID, COL_LINK_TRACE_ID, COL_NAME,
    COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID, COL_PARENT_SPAN_ID, COL_ROOT_SERVICE_NAME,
    COL_ROOT_SPAN_NAME, COL_SPAN_ID, COL_STATUS_CODE, COL_STATUS_MESSAGE, COL_TRACE_DURATION,
    COL_TRACE_ID, CompareSpec, DataType, DurationNanos, EVENT_ATTR_PREFIX, Field, FieldExpr,
    INSTRUMENTATION_ATTR_PREFIX, Intrinsic, LINK_ATTR_PREFIX, MetricLabel, MetricPoint, Pipeline,
    Query, RankLimit, RecordBatch, Result, Scope, SpanMatcher, SpansetExpr, Time,
    TraceMetricExemplar, TraceMetricLabelType, TraceMetricSeries, TraceqlError, UnixNano,
    bytes_to_hex, compare_row, compare_row_attr_values, f64_from_i64, f64_from_u64, f64_from_usize,
    fixed_16, i32_value, i64_value, is_inert_metric_stage, kind_enum_name,
    nested_projection_matcher, quantile_label, rank_stage_limit, status_enum_name,
    string_array_value, unsupported_metric_pipeline,
};

// Metric Bucket
#[derive(Clone, Default)]
pub(crate) struct MetricBucket {
    pub(crate) count: u64,
    pub(crate) sum: f64,
    sum_compensation: f64,
    mean: Option<f64>,
    mean_compensation: f64,
    pub(crate) min: Option<f64>,
    pub(crate) max: Option<f64>,
    pub(crate) values: Vec<f64>,
    pub(crate) exemplars: Vec<TraceMetricExemplar>,
}

struct CompensatedSum {
    sum: f64,
    compensation: f64,
}

impl MetricBucket {
    pub(crate) fn record(&mut self, value: Option<f64>, exemplar: Option<TraceMetricExemplar>) {
        self.count += 1;
        if let Some(exemplar) = exemplar {
            self.exemplars.push(exemplar);
        }
        let Some(value) = value else {
            return;
        };
        let accumulated = kahan_increment(self.sum, value - self.sum_compensation, 0.0);
        self.sum = accumulated.sum;
        self.sum_compensation = accumulated.compensation;
        if let Some(mean) = self.mean {
            let count =
                f64_from_u64(self.count).expect("u64 values have finite f64 representations");
            let increment = value / count - mean / count;
            if !(mean.is_infinite()
                && (increment.is_finite()
                    || (increment.is_infinite()
                        && mean.is_sign_positive() == increment.is_sign_positive())))
            {
                let accumulated = kahan_increment(mean, increment, self.mean_compensation);
                self.mean = Some(accumulated.sum);
                self.mean_compensation = accumulated.compensation;
            }
        } else {
            self.mean = Some(value);
        }
        self.min = Some(self.min.map_or(value, |min| min.min(value)));
        self.max = Some(self.max.map_or(value, |max| max.max(value)));
        self.values.push(value);
    }

    pub(crate) fn average(&self) -> f64 {
        if self.count == 0 {
            f64::NAN
        } else {
            self.mean.unwrap_or(f64::NAN) + self.mean_compensation
        }
    }

    pub(crate) fn quantile(&self, quantile: f64) -> Result<f64> {
        if self.values.is_empty() {
            return Ok(0.0);
        }
        let mut values = self.values.clone();
        values.sort_by(f64::total_cmp);
        if values.len() == 1 {
            return Ok(values[0]);
        }
        let mut wanted =
            usize_from_integer_f64((quantile * f64_from_usize(values.len())?).ceil())?.max(1);
        let mut index = 0;
        let mut previous: Option<f64> = None;
        while index < values.len() {
            let boundary = values[index];
            let mut count = 1;
            while index + count < values.len()
                && values[index + count].to_bits() == boundary.to_bits()
            {
                count += 1;
            }
            if wanted >= count {
                wanted -= count;
                if wanted == 0 {
                    return Ok(boundary);
                }
            } else {
                let upper = boundary.log2();
                let lower = previous.map_or(upper - 1.0, f64::log2);
                return Ok((lower
                    + (upper - lower) * f64_from_usize(wanted)? / f64_from_usize(count)?)
                .exp2());
            }
            previous = Some(boundary);
            index += count;
        }
        Ok(*values.last().expect("nonempty quantile observations"))
    }
}

// Tempo's shared Kahan/Neumaier increment, including its infinity branch.
fn kahan_increment(sum: f64, increment: f64, mut compensation: f64) -> CompensatedSum {
    let result = sum + increment;
    if result.is_infinite() {
        compensation = 0.0;
    } else if sum.abs() >= increment.abs() {
        compensation += (sum - result) + increment;
    } else {
        compensation += (increment - result) + sum;
    }
    CompensatedSum {
        sum: result,
        compensation,
    }
}

// Metric Exemplar
pub(crate) fn metric_exemplar(
    batch: &RecordBatch,
    row: usize,
    timestamp_ns: i64,
    value: f64,
    fields: &[Field],
) -> Result<TraceMetricExemplar> {
    // Tempo uses its unpadded hexadecimal trace intrinsic, plus attributes
    // fetched for the query. Span IDs are included only when queried.
    let trace_id = bytes_to_hex(&fixed_16(batch, COL_TRACE_ID, row)?);
    let trace_id = trace_id.trim_start_matches('0');
    let mut labels = vec![MetricLabel::new(
        "trace:id",
        if trace_id.is_empty() { "0" } else { trace_id },
    )];
    let projected = metric_labels(batch, row, fields)?;
    labels.extend(
        projected
            .labels
            .into_iter()
            .filter(|label| label.key != "trace:id"),
    );
    Ok(TraceMetricExemplar {
        labels,
        label_types: projected.types,
        value,
        timestamp_ns,
    })
}

// Metric Exemplars
pub(crate) struct ExemplarBuckets {
    counts: Vec<usize>,
    total: usize,
    limit: usize,
    start_ms: i64,
    end_ms: i64,
    width_ms: i64,
}

impl ExemplarBuckets {
    pub(crate) fn new(limit: usize, start_ns: i64, end_ns: i64) -> Self {
        let count = (limit / 2).max(1);
        let lower_bound = start_ns.div_euclid(1_000_000);
        let upper_bound = end_ns.div_euclid(1_000_000);
        let width_ms =
            ((upper_bound - lower_bound) / i64::try_from(count).unwrap_or(i64::MAX)).max(1);
        Self {
            counts: vec![0; count],
            total: 0,
            limit,
            start_ms: lower_bound,
            end_ms: upper_bound,
            width_ms,
        }
    }

    pub(crate) fn admit(&mut self, timestamp_ns: i64) -> bool {
        let timestamp = timestamp_ns.div_euclid(1_000_000);
        if self.total >= self.limit || timestamp < self.start_ms || timestamp > self.end_ms {
            return false;
        }
        let last = self.counts.len() - 1;
        let bucket = usize::try_from((timestamp - self.start_ms) / self.width_ms)
            .unwrap_or(last)
            .min(last);
        if self.counts[bucket] >= 2 {
            return false;
        }
        self.counts[bucket] += 1;
        self.total += 1;
        true
    }
}

pub(crate) fn metric_exemplars(
    buckets: &[MetricBucket],
    max_exemplars: usize,
    start_ns: i64,
    end_ns: i64,
) -> Vec<TraceMetricExemplar> {
    let mut admission = ExemplarBuckets::new(max_exemplars, start_ns, end_ns);
    buckets
        .iter()
        .flat_map(|bucket| &bucket.exemplars)
        .filter(|exemplar| admission.admit(exemplar.timestamp_ns))
        .cloned()
        .collect()
}

// Metric Field Column
pub(crate) fn metric_field_column(field: &Field) -> Result<String> {
    match field.scope {
        Scope::Both | Scope::Resource if field.key == "service.name" => {
            Ok(COL_ROOT_SERVICE_NAME.to_string())
        }
        Scope::Both | Scope::Span | Scope::Resource => Ok(format!("{ATTR_PREFIX}{}", field.key)),
        Scope::Event => Ok(format!("{ATTR_PREFIX}{EVENT_ATTR_PREFIX}{}", field.key)),
        Scope::Link => Ok(format!("{ATTR_PREFIX}{LINK_ATTR_PREFIX}{}", field.key)),
        Scope::Instrumentation => Ok(format!(
            "{ATTR_PREFIX}{INSTRUMENTATION_ATTR_PREFIX}{}",
            field.key
        )),
        Scope::Intrinsic(Intrinsic::Name) => Ok(COL_NAME.to_string()),
        Scope::Intrinsic(Intrinsic::Duration) => Ok(COL_DURATION.to_string()),
        Scope::Intrinsic(Intrinsic::Id) => Ok(COL_SPAN_ID.to_string()),
        Scope::Intrinsic(Intrinsic::ParentId) => Ok(COL_PARENT_SPAN_ID.to_string()),
        Scope::Intrinsic(Intrinsic::ChildCount) => Ok(COL_CHILD_COUNT.to_string()),
        Scope::Intrinsic(Intrinsic::NestedSetLeft) => Ok(COL_NS_LEFT.to_string()),
        Scope::Intrinsic(Intrinsic::NestedSetRight) => Ok(COL_NS_RIGHT.to_string()),
        Scope::Intrinsic(Intrinsic::NestedSetParent) => Ok(COL_PARENT_ID.to_string()),
        Scope::Intrinsic(Intrinsic::Kind) => Ok(COL_KIND.to_string()),
        Scope::Intrinsic(Intrinsic::Status) => Ok(COL_STATUS_CODE.to_string()),
        Scope::Intrinsic(Intrinsic::StatusMessage) => Ok(COL_STATUS_MESSAGE.to_string()),
        Scope::Intrinsic(Intrinsic::TraceId) => Ok(COL_TRACE_ID.to_string()),
        Scope::Intrinsic(Intrinsic::TraceDuration) => Ok(COL_TRACE_DURATION.to_string()),
        Scope::Intrinsic(Intrinsic::TraceRootService) => Ok(COL_ROOT_SERVICE_NAME.to_string()),
        Scope::Intrinsic(Intrinsic::TraceRootName) => Ok(COL_ROOT_SPAN_NAME.to_string()),
        Scope::Intrinsic(Intrinsic::InstrumentationName) => {
            Ok(COL_INSTRUMENTATION_NAME.to_string())
        }
        Scope::Intrinsic(Intrinsic::InstrumentationVersion) => {
            Ok(COL_INSTRUMENTATION_VERSION.to_string())
        }
        Scope::Intrinsic(Intrinsic::EventName) => Ok(COL_EVENT_NAME.to_string()),
        Scope::Intrinsic(Intrinsic::EventTimeSinceStart) => {
            Ok(COL_EVENT_TIME_SINCE_START.to_string())
        }
        Scope::Intrinsic(Intrinsic::LinkTraceId) => Ok(COL_LINK_TRACE_ID.to_string()),
        Scope::Intrinsic(Intrinsic::LinkSpanId) => Ok(COL_LINK_SPAN_ID.to_string()),
        _ => Err(TraceqlError::Unsupported(format!(
            "metrics by() field {field:?} is not supported yet"
        ))),
    }
}

// Metric Field Number
/// The numeric value of a metric fold field for one row, in its stored type.
pub(crate) enum MetricFieldNumber {
    Int(i64),
    Float(f64),
}

/// Looks up the numeric value of a metric fold field for one row.
///
/// This function returns `Ok(None)` when the field has more than one attribute
/// value (Tempo admits no array element as a scalar observation), when the
/// value is not numeric, or when the row's projected column is NULL, that is,
/// when the attribute is absent.
pub(crate) fn metric_field_number(
    batch: &RecordBatch,
    row: usize,
    field: &Field,
) -> Result<Option<MetricFieldNumber>> {
    let values = super::compare_row(batch, row, super::UnixNano(0))?;
    let raw = super::compare_row_attr_values(&values, &field.scope, &field.key);
    if raw.len() > 1 {
        return Ok(None);
    }
    if let Some(value) = raw.first() {
        return Ok(match value {
            AttrValue::Int(value) => Some(MetricFieldNumber::Int(*value)),
            AttrValue::Float(value) => Some(MetricFieldNumber::Float(*value)),
            AttrValue::Unsupported(_)
            | AttrValue::Array(_)
            | AttrValue::Bool(_)
            | AttrValue::Str(_) => None,
        });
    }
    let column = metric_field_column(field)?;
    let array = batch
        .column_by_name(&column)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {column}")))?;
    if array.is_null(row) {
        return Ok(None);
    }
    Ok(match array.data_type() {
        DataType::Int64 => Some(MetricFieldNumber::Int(
            array
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(row),
        )),
        DataType::Int32 => Some(MetricFieldNumber::Int(i64::from(
            array
                .as_primitive::<arrow::datatypes::Int32Type>()
                .value(row),
        ))),
        DataType::Float64 => Some(MetricFieldNumber::Float(
            array
                .as_primitive::<arrow::datatypes::Float64Type>()
                .value(row),
        )),
        _ => None,
    })
}

// Metric Filter
#[derive(Clone, Copy)]
pub(crate) struct MetricFilter {
    pub(crate) op: crate::ast::ComparisonOp,
    pub(crate) value: f64,
}

pub(crate) fn metric_filter(op: crate::ast::ComparisonOp, value: f64) -> Result<MetricFilter> {
    if !value.is_finite() {
        return Err(TraceqlError::Plan(
            "metric comparison filter value is not finite".into(),
        ));
    }
    match op {
        crate::ast::ComparisonOp::Eq
        | crate::ast::ComparisonOp::Neq
        | crate::ast::ComparisonOp::Lt
        | crate::ast::ComparisonOp::Lte
        | crate::ast::ComparisonOp::Gt
        | crate::ast::ComparisonOp::Gte => Ok(MetricFilter { op, value }),
        crate::ast::ComparisonOp::Re | crate::ast::ComparisonOp::Nre => Err(
            TraceqlError::Unsupported("regex filter on metric scalar is not supported".into()),
        ),
    }
}

// Metric Filter Passes
pub(crate) fn metric_filter_passes(value: f64, filter: MetricFilter) -> bool {
    let ordering = value.partial_cmp(&filter.value);
    match filter.op {
        crate::ast::ComparisonOp::Eq => ordering.is_some_and(std::cmp::Ordering::is_eq),
        crate::ast::ComparisonOp::Neq => !ordering.is_some_and(std::cmp::Ordering::is_eq),
        crate::ast::ComparisonOp::Lt => ordering.is_some_and(std::cmp::Ordering::is_lt),
        crate::ast::ComparisonOp::Lte => ordering.is_some_and(|ordering| !ordering.is_gt()),
        crate::ast::ComparisonOp::Gt => ordering.is_some_and(std::cmp::Ordering::is_gt),
        crate::ast::ComparisonOp::Gte => ordering.is_some_and(|ordering| !ordering.is_lt()),
        crate::ast::ComparisonOp::Re | crate::ast::ComparisonOp::Nre => false,
    }
}

// Metric Function
#[derive(Clone, Copy)]
pub(crate) enum MetricFunction {
    Rate,
    CountOverTime,
    SumOverTime,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    HistogramOverTime,
    QuantileOverTime,
}

// Metric Label Key
/// The series label key for a `by(<field>)` clause, as in real Tempo.
///
/// The key is the FULLY-SCOPED attribute name, such as
/// `resource.service.name` or `span.http.method`, not the scope-stripped key.
/// Grafana's Traces Drilldown keys its per-attribute breakdown panels on this
/// exact name. A stripped key such as `service.name` leaves the breakdown
/// blank, even though the data below it is correct. `tempo_differential`
/// verified this against real Tempo.
pub(crate) fn metric_label_key(field: &Field) -> String {
    let prefix = match &field.scope {
        Scope::Resource => "resource.",
        Scope::Span => "span.",
        Scope::Event => "event.",
        Scope::Link => "link.",
        Scope::Both => ".",
        Scope::Parent => "parent.",
        Scope::Instrumentation => "instrumentation.",
        // Intrinsics (name/status/kind/duration/trace:* …) are referenced by
        // their own names, not a scoped attribute key — keep the parser key.
        Scope::Intrinsic(intrinsic) => {
            return match intrinsic {
                Intrinsic::Name => "name",
                Intrinsic::Duration => "duration",
                Intrinsic::Status => "status",
                Intrinsic::StatusMessage => "statusMessage",
                Intrinsic::Kind => "kind",
                Intrinsic::NestedSetLeft => "nestedSetLeft",
                Intrinsic::NestedSetRight => "nestedSetRight",
                Intrinsic::NestedSetParent => "nestedSetParent",
                Intrinsic::Id => "span:id",
                Intrinsic::ParentId => "span:parentID",
                Intrinsic::ChildCount => "span:childCount",
                Intrinsic::TraceId => "trace:id",
                Intrinsic::EventName => "event:name",
                Intrinsic::EventTimeSinceStart => "event:timeSinceStart",
                Intrinsic::LinkTraceId => "link:traceID",
                Intrinsic::LinkSpanId => "link:spanID",
                Intrinsic::InstrumentationName => "instrumentation:name",
                Intrinsic::InstrumentationVersion => "instrumentation:version",
                _ => &field.key,
            }
            .into();
        }
    };
    format!("{prefix}{}", field.key)
}

// Metric Label Value
pub(crate) fn metric_label_value(batch: &RecordBatch, column: &str, row: usize) -> Result<String> {
    let array = batch
        .column_by_name(column)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {column}")))?;
    if array.is_null(row) {
        return Ok("nil".to_owned());
    }
    match array.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            string_array_value(array.as_ref(), row)
                .ok_or_else(|| TraceqlError::Exec("unsupported string column type".into()))
        }
        DataType::Dictionary(_, value_type)
            if matches!(
                value_type.as_ref(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) =>
        {
            string_array_value(array.as_ref(), row)
                .ok_or_else(|| TraceqlError::Exec("unsupported string column type".into()))
        }
        DataType::Int64 => Ok(array
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(row)
            .to_string()),
        DataType::Float64 => Ok(array
            .as_primitive::<arrow::datatypes::Float64Type>()
            .value(row)
            .to_string()),
        DataType::Boolean => Ok(array.as_boolean().value(row).to_string()),
        DataType::Int32 => Ok(array
            .as_primitive::<arrow::datatypes::Int32Type>()
            .value(row)
            .to_string()),
        DataType::FixedSizeBinary(_) => Ok(bytes_to_hex(array.as_fixed_size_binary().value(row))),
        DataType::Struct(_) => {
            let key = column.strip_prefix(super::ATTR_PREFIX).ok_or_else(|| {
                TraceqlError::Exec("typed attributes require an attribute column".into())
            })?;
            let value = super::row_attrs(batch, row)?
                .into_iter()
                .find(|attribute| attribute.key == key)
                .map(|attribute| match attribute.value {
                    super::AttrValue::Unsupported(_) => "nil".into(),
                    super::AttrValue::Array(value) => format!("{value:?}"),
                    super::AttrValue::Str(value) => value,
                    super::AttrValue::Int(value) => value.to_string(),
                    super::AttrValue::Float(value) => value.to_string(),
                    super::AttrValue::Bool(value) => value.to_string(),
                });
            Ok(value.unwrap_or_else(|| "nil".into()))
        }
        other => Err(TraceqlError::Exec(format!(
            "unsupported metrics label column type {other:?}"
        ))),
    }
}

// Metric Labels
/// Keep the display labels and scalar types together in the grouping key.
/// Without the type component, integer 1 and string "1" share a bucket.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct MetricLabels {
    pub(crate) labels: Vec<MetricLabel>,
    pub(crate) types: BTreeMap<String, TraceMetricLabelType>,
}

struct TypedMetricLabelValue {
    value: String,
    kind: Option<TraceMetricLabelType>,
}

/// Tempo 3.0.3's frontend `StaticFromAnyValue` decodes only scalar values.
/// Its raw worker serializes arrays, but frontend decoding turns them into
/// `StaticNil`, retaining the label key. Keep this conversion out of raw
/// metric grouping, selectors, and search attribute export.
pub(crate) fn decode_frontend_metric_labels(
    labels: &mut [MetricLabel],
    types: &mut BTreeMap<String, TraceMetricLabelType>,
) {
    for label in labels {
        if types.get(&label.key) == Some(&TraceMetricLabelType::Array) {
            label.value = "nil".into();
            types.insert(label.key.clone(), TraceMetricLabelType::Nil);
        }
    }
}

pub(crate) fn metric_labels(
    batch: &RecordBatch,
    row: usize,
    fields: &[Field],
) -> Result<MetricLabels> {
    let mut labels = Vec::new();
    let mut types = BTreeMap::new();
    let values = compare_row(batch, row, UnixNano(0))?;
    for field in fields {
        let key = metric_label_key(field);
        let raw = compare_row_attr_values(&values, &field.scope, &field.key);
        // Packed fields preserve original types even when promoted columns
        // stringify heterogeneous values. Scopeless lookup prefers the span.
        let elements = match raw.as_slice() {
            [AttrValue::Array(values)] => Some(values.iter().collect::<Vec<_>>()),
            values if values.len() > 1 => Some(values.to_vec()),
            _ => None,
        };
        if let Some(elements) = elements {
            types.insert(key.clone(), TraceMetricLabelType::Array);
            labels.push(MetricLabel::new(key, serde_json::json!({"arrayValue":{"values":elements.into_iter().map(attribute_json).collect::<Vec<_>>()}}).to_string()));
            continue;
        }
        if let Some(value) = raw.first() {
            let label_value = match value {
                AttrValue::Unsupported(_) | AttrValue::Array(_) => {
                    unreachable!("array labels are handled above")
                }
                AttrValue::Str(value) => TypedMetricLabelValue {
                    value: value.clone(),
                    kind: None,
                },
                AttrValue::Int(value) => TypedMetricLabelValue {
                    value: value.to_string(),
                    kind: Some(TraceMetricLabelType::Int),
                },
                AttrValue::Float(value) => TypedMetricLabelValue {
                    value: value.to_string(),
                    kind: Some(TraceMetricLabelType::Double),
                },
                AttrValue::Bool(value) => TypedMetricLabelValue {
                    value: value.to_string(),
                    kind: Some(TraceMetricLabelType::Bool),
                },
            };
            if let Some(kind) = label_value.kind {
                types.insert(key.clone(), kind);
            }
            labels.push(MetricLabel::new(key, label_value.value));
            continue;
        }
        let column = metric_field_column(field)?;
        let Some(array) = batch.column_by_name(&column) else {
            continue;
        };
        if array.is_null(row) {
            continue;
        }
        let value = match field.scope {
            Scope::Intrinsic(Intrinsic::Status) => {
                status_enum_name(i32_value(batch, &column, row)?).to_string()
            }
            Scope::Intrinsic(Intrinsic::Kind) => {
                kind_enum_name(i32_value(batch, &column, row)?).to_string()
            }
            Scope::Intrinsic(
                Intrinsic::Duration | Intrinsic::TraceDuration | Intrinsic::EventTimeSinceStart,
            ) => metric_duration_label(i64_value(batch, &column, row)?),
            _ => metric_label_value(batch, &column, row)?,
        };
        let kind = match field.scope {
            Scope::Intrinsic(
                Intrinsic::Status
                | Intrinsic::Kind
                | Intrinsic::Duration
                | Intrinsic::TraceDuration
                | Intrinsic::EventTimeSinceStart,
            ) => None,
            _ => match array.data_type() {
                DataType::Int64 | DataType::Int32 => Some(TraceMetricLabelType::Int),
                DataType::Float64 => Some(TraceMetricLabelType::Double),
                DataType::Boolean => Some(TraceMetricLabelType::Bool),
                _ => None,
            },
        };
        if let Some(kind) = kind {
            types.insert(key.clone(), kind);
        }
        labels.push(MetricLabel::new(key, value));
    }
    Ok(MetricLabels { labels, types })
}

fn attribute_json(value: &AttrValue) -> serde_json::Value {
    match value {
        AttrValue::Unsupported(value) => {
            serde_json::from_str(value).expect("opaque attribute contains AnyValue JSON")
        }
        AttrValue::Str(value) => serde_json::json!({"stringValue":value}),
        AttrValue::Int(value) => serde_json::json!({"intValue":value.to_string()}),
        AttrValue::Bool(value) => serde_json::json!({"boolValue":value}),
        AttrValue::Float(value) => {
            let number = if value.is_nan() {
                serde_json::json!("NaN")
            } else if *value == f64::INFINITY {
                serde_json::json!("Infinity")
            } else if *value == f64::NEG_INFINITY {
                serde_json::json!("-Infinity")
            } else {
                serde_json::json!(value)
            };
            serde_json::json!({"doubleValue":number})
        }
        AttrValue::Array(values) => {
            serde_json::json!({"arrayValue":{"values":values.iter().map(attribute_json).collect::<Vec<_>>()}})
        }
    }
}

// Metric Nested Projection Matchers
pub(crate) fn metric_nested_projection_matchers(metric: &MetricPlan) -> Vec<SpanMatcher> {
    let mut out = Vec::new();
    for field in metric.by.iter().chain(metric.value.iter()) {
        if let Some(matcher) = nested_projection_matcher(field)
            && !out.contains(&matcher)
        {
            out.push(matcher);
        }
    }
    out
}

// Metric Numeric Value
/// Extracts the numeric value of a metric fold field for one row.
///
/// This function returns `Ok(None)` when the row's value field is NULL, that
/// is, when the target attribute is absent for that span. The caller can then
/// skip the span instead of folding a false `0.0` into sum, min, max, avg, or
/// histogram. Tempo's Static.Float returns NaN for arrays: none of their
/// elements is admitted as a scalar numeric observation.
pub(crate) fn metric_numeric_value(
    batch: &RecordBatch,
    row: usize,
    field: &Field,
) -> Result<Option<f64>> {
    let value = match metric_field_number(batch, row, field)? {
        Some(MetricFieldNumber::Int(value)) => f64_from_i64(value),
        Some(MetricFieldNumber::Float(value)) => value,
        None => return Ok(None),
    };
    if value.is_nan() {
        return Ok(None);
    }
    Ok(Some(
        if matches!(
            field.scope,
            super::Scope::Intrinsic(super::Intrinsic::Duration)
        ) {
            value / 1_000_000_000.0
        } else {
            value
        },
    ))
}

// Metric Pipeline Parts
pub(crate) struct MetricPipelineParts<'a> {
    pub(crate) aggregate: Option<&'a Aggregate>,
    pub(crate) by: Vec<Field>,
    pub(crate) stages: Vec<Pipeline>,
    pub(crate) compare: Option<CompareSpec>,
}

pub(crate) fn metric_pipeline_parts(
    pipeline: &[Pipeline],
) -> Result<Option<MetricPipelineParts<'_>>> {
    let mut aggregate = None;
    let mut by = None;
    let mut stages = Vec::new();
    let mut compare = None;
    for stage in pipeline {
        match stage {
            Pipeline::Aggregate(value) if aggregate.is_none() => aggregate = Some(value),
            Pipeline::By(value) if by.is_none() && compare.is_none() => by = Some(value.clone()),
            Pipeline::Filter { op, value } if aggregate.is_some() && compare.is_none() => {
                metric_filter(*op, *value)?;
                stages.push(stage.clone());
            }
            stage @ (Pipeline::TopK(_) | Pipeline::BottomK(_))
                if aggregate.is_some() && compare.is_none() =>
            {
                rank_limit(stage)?;
                stages.push(stage.clone());
            }
            Pipeline::Compare {
                selection,
                top_n,
                start,
                end,
            } if compare.is_none() => {
                compare = Some(CompareSpec {
                    selection: (**selection).clone(),
                    top_n: *top_n,
                    start: start.map(UnixNano),
                    end: end.map(UnixNano),
                });
            }
            _ => return Ok(None),
        }
    }
    // A compare-only pipeline has no aggregate but is still a valid metric.
    if aggregate.is_none() && compare.is_none() {
        return Ok(None);
    }
    Ok(Some(MetricPipelineParts {
        aggregate,
        by: by.unwrap_or_default(),
        stages,
        compare,
    }))
}

// Metric Plan
pub(crate) struct MetricPlan {
    pub(crate) function: MetricFunction,
    pub(crate) value: Option<Field>,
    pub(crate) quantiles: Vec<f64>,
    pub(crate) by: Vec<Field>,
    pub(crate) exemplar_fields: Vec<Field>,
    pub(crate) stages: Vec<Pipeline>,
    pub(crate) sampling_factor: f64,
    pub(crate) spanset_pipeline_had_input: bool,
    pub(crate) frontend_labels: bool,
    pub(crate) instant: bool,
    pub(crate) spanset_pipeline: Vec<Pipeline>,
    pub(crate) compare: Option<CompareSpec>,
}

pub(crate) fn metric_plan(q: &Query) -> Result<MetricPlan> {
    let metric_start = q
        .pipeline
        .iter()
        .position(is_metric_stage)
        .ok_or_else(unsupported_metric_pipeline)?;
    let spanset_pipeline = q.pipeline[..metric_start].to_vec();
    let metric_pipeline = &q.pipeline[metric_start..];
    let normalized_pipeline;
    let pipeline = if metric_pipeline.iter().any(is_inert_metric_stage) {
        normalized_pipeline = metric_pipeline
            .iter()
            .filter(|stage| !is_inert_metric_stage(stage))
            .cloned()
            .collect::<Vec<_>>();
        normalized_pipeline.as_slice()
    } else {
        metric_pipeline
    };

    let parts = metric_pipeline_parts(pipeline)?;
    // A `compare()` stage is a standalone metric (no `*_over_time()` aggregate
    // needed): `{outer} | compare({selection}, topN)`. When present it takes
    // precedence and the aggregate, if any, is ignored.
    if let Some(compare) = parts.as_ref().and_then(|parts| parts.compare.clone()) {
        let mut plan = metric_plan_with_compare(compare);
        plan.spanset_pipeline = spanset_pipeline;
        return Ok(plan);
    }
    let Some(parts) = parts else {
        return Err(unsupported_metric_pipeline());
    };
    let Some(aggregate) = parts.aggregate else {
        return Err(unsupported_metric_pipeline());
    };
    let mut plan = metric_plan_for(aggregate, parts.by, parts.stages)?;
    plan.spanset_pipeline = spanset_pipeline;
    collect_exemplar_fields(&q.root, &mut plan.exemplar_fields);
    for field in plan.by.iter().chain(plan.value.iter()) {
        if !plan.exemplar_fields.contains(field) {
            plan.exemplar_fields.push(field.clone());
        }
    }
    Ok(plan)
}

fn collect_exemplar_fields(expr: &SpansetExpr, fields: &mut Vec<Field>) {
    match expr {
        SpansetExpr::Selector(expr) => collect_selector_fields(expr, fields),
        SpansetExpr::And(lhs, rhs)
        | SpansetExpr::Or(lhs, rhs)
        | SpansetExpr::Structural { lhs, rhs, .. } => {
            collect_exemplar_fields(lhs, fields);
            collect_exemplar_fields(rhs, fields);
        }
    }
}

fn collect_selector_fields(expr: &FieldExpr, fields: &mut Vec<Field>) {
    match expr {
        FieldExpr::ExpressionComparison { lhs, rhs, .. } => {
            let mut dependencies = Vec::new();
            lhs.collect_fields(&mut dependencies);
            rhs.collect_fields(&mut dependencies);
            for field in dependencies {
                add_field(field, fields);
            }
        }
        FieldExpr::Comparison { lhs, .. } | FieldExpr::Field(lhs) => add_field(lhs, fields),
        FieldExpr::FieldComparison { lhs, rhs, .. } => {
            add_field(lhs, fields);
            add_field(rhs, fields);
        }
        FieldExpr::And(lhs, rhs) | FieldExpr::Or(lhs, rhs) => {
            collect_selector_fields(lhs, fields);
            collect_selector_fields(rhs, fields);
        }
        FieldExpr::Not(inner) => collect_selector_fields(inner, fields),
        FieldExpr::Const(_) => {}
    }
}

fn add_field(field: &Field, fields: &mut Vec<Field>) {
    if !fields.contains(field) {
        fields.push(field.clone());
    }
}

fn is_metric_stage(stage: &Pipeline) -> bool {
    matches!(stage, Pipeline::Compare { .. })
        || matches!(stage, Pipeline::Aggregate(aggregate) if aggregate.is_metric())
}

// Metric Plan For
struct MetricPlanSpec {
    function: MetricFunction,
    value: Option<Field>,
    quantiles: Vec<f64>,
}

pub(crate) fn metric_plan_for(
    aggregate: &Aggregate,
    by: Vec<Field>,
    stages: Vec<Pipeline>,
) -> Result<MetricPlan> {
    let MetricPlanSpec {
        function,
        value,
        quantiles,
    } = match aggregate {
        Aggregate::Rate => MetricPlanSpec {
            function: MetricFunction::Rate,
            value: None,
            quantiles: Vec::new(),
        },
        Aggregate::CountOverTime => MetricPlanSpec {
            function: MetricFunction::CountOverTime,
            value: None,
            quantiles: Vec::new(),
        },
        Aggregate::SumOverTime(field) => MetricPlanSpec {
            function: MetricFunction::SumOverTime,
            value: Some(field.clone()),
            quantiles: Vec::new(),
        },
        Aggregate::AvgOverTime(field) => MetricPlanSpec {
            function: MetricFunction::AvgOverTime,
            value: Some(field.clone()),
            quantiles: Vec::new(),
        },
        Aggregate::MinOverTime(field) => MetricPlanSpec {
            function: MetricFunction::MinOverTime,
            value: Some(field.clone()),
            quantiles: Vec::new(),
        },
        Aggregate::MaxOverTime(field) => MetricPlanSpec {
            function: MetricFunction::MaxOverTime,
            value: Some(field.clone()),
            quantiles: Vec::new(),
        },
        Aggregate::HistogramOverTime(field) => MetricPlanSpec {
            function: MetricFunction::HistogramOverTime,
            value: Some(field.clone()),
            quantiles: Vec::new(),
        },
        Aggregate::QuantileOverTime { field, quantiles } => MetricPlanSpec {
            function: MetricFunction::QuantileOverTime,
            value: Some(field.clone()),
            quantiles: quantiles.clone(),
        },
        Aggregate::Expression { .. }
        | Aggregate::Count
        | Aggregate::Avg(_)
        | Aggregate::Sum(_)
        | Aggregate::Min(_)
        | Aggregate::Max(_) => {
            return Err(TraceqlError::Unsupported(
                "traceql metrics: expected supported *_over_time() metric".into(),
            ));
        }
    };
    Ok(MetricPlan {
        function,
        value,
        quantiles,
        by,
        exemplar_fields: Vec::new(),
        stages,
        spanset_pipeline: Vec::new(),
        sampling_factor: 1.0,
        spanset_pipeline_had_input: false,
        frontend_labels: false,
        instant: false,
        compare: None,
    })
}

// Metric Plan With Compare
/// Builds the `MetricPlan` for a `compare()` stage.
///
/// The `function`, `value`, and `by` fields are inert placeholders.
/// `query_range_compare` reads `compare` directly, and never runs the
/// `*_over_time()` machinery.
pub(crate) fn metric_plan_with_compare(compare: CompareSpec) -> MetricPlan {
    MetricPlan {
        compare: Some(compare),
        ..count_over_time_plan()
    }
}

// Metric Series For Group
pub(crate) fn metric_series_for_group(
    labels: MetricLabels,
    buckets: &[MetricBucket],
    metric: &MetricPlan,
    grid: MetricGrid,
    step: Time,
    exemplar_range: ExemplarRange,
    metric_policy: MetricExemplarPolicy<'_>,
) -> Result<Vec<TraceMetricSeries>> {
    let start_ns = grid.start_ns;
    let step_ns = grid.step_ns;
    let exemplars = metric_exemplars(
        buckets,
        metric_policy.max_exemplars_per_series,
        exemplar_range.start_ns,
        exemplar_range.end_ns,
    );
    if matches!(metric.function, MetricFunction::QuantileOverTime) {
        return metric
            .quantiles
            .iter()
            .map(|quantile| {
                let mut labels = labels.clone();
                labels
                    .labels
                    .insert(0, MetricLabel::new("p", quantile_label(*quantile)));
                let points = (0..buckets.len())
                    .map(|bucket_index| {
                        let ts =
                            start_ns + i64::try_from(bucket_index).unwrap_or(i64::MAX) * step_ns;
                        Ok(MetricPoint::new(
                            ts,
                            buckets[bucket_index].quantile(*quantile)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(TraceMetricSeries {
                    label_types: labels.types.clone(),
                    labels: labels.labels.clone(),
                    points,
                    exemplars: exemplars.clone(),
                })
            })
            .collect();
    }
    if matches!(metric.function, MetricFunction::HistogramOverTime) {
        return histogram_series_for_group(
            labels,
            buckets,
            start_ns,
            step_ns,
            &exemplars,
            metric_policy.histogram_buckets,
        );
    }

    let points = buckets
        .iter()
        .enumerate()
        .map(|indexed_bucket| {
            let bucket_index = indexed_bucket.0;
            let bucket = indexed_bucket.1;
            let ts = start_ns + i64::try_from(bucket_index).unwrap_or(i64::MAX) * step_ns;
            let value = match metric.function {
                MetricFunction::Rate => f64_from_u64(bucket.count)? / step.secs_f64(),
                MetricFunction::CountOverTime => f64_from_u64(bucket.count)?,
                MetricFunction::SumOverTime => {
                    if bucket.count == 0 {
                        f64::NAN
                    } else {
                        bucket.sum
                    }
                }
                MetricFunction::AvgOverTime => bucket.average(),
                MetricFunction::MinOverTime => bucket.min.unwrap_or(f64::NAN),
                MetricFunction::MaxOverTime => bucket.max.unwrap_or(f64::NAN),
                MetricFunction::HistogramOverTime | MetricFunction::QuantileOverTime => {
                    unreachable!("handled above")
                }
            };
            Ok(MetricPoint::new(ts, value))
        })
        .collect::<Result<Vec<_>>>()?;
    let points = points
        .into_iter()
        .filter(|point| !point.value.is_nan())
        .collect::<Vec<_>>();
    if points.is_empty() {
        return Ok(Vec::new());
    }
    Ok(vec![TraceMetricSeries {
        label_types: labels.types.clone(),
        labels: labels.labels,
        points,
        exemplars,
    }])
}

// Metric Timing
#[derive(Clone, Copy)]
pub(crate) struct MetricGrid {
    pub(crate) start_ns: i64,
    pub(crate) step_ns: i64,
}

#[derive(Clone, Copy)]
pub(crate) struct ExemplarRange {
    pub(crate) start_ns: i64,
    pub(crate) end_ns: i64,
}

#[derive(Clone, Copy)]
pub(crate) struct MetricExemplarPolicy<'a> {
    pub(crate) max_exemplars_per_series: usize,
    pub(crate) histogram_buckets: &'a [Time],
}

impl<'a> MetricExemplarPolicy<'a> {
    pub(crate) fn new(max_exemplars_per_series: usize, histogram_buckets: &'a [Time]) -> Self {
        Self {
            max_exemplars_per_series,
            histogram_buckets,
        }
    }
}

// Metric Log2 Bucket Value
/// Tempo buckets integer attributes at powers of two; duration buckets are
/// based on integer nanoseconds, then expressed in seconds. Floats are not
/// supported by the pinned histogram aggregator.
pub(crate) fn metric_log2_bucket_value(
    batch: &RecordBatch,
    row: usize,
    field: &Field,
) -> Result<Option<f64>> {
    let Some(MetricFieldNumber::Int(value)) = metric_field_number(batch, row, field)? else {
        return Ok(None);
    };
    let Ok(value) = u64::try_from(value) else {
        return Ok(None);
    };
    if value < 2 {
        return Ok(None);
    }
    let bucket = value
        .checked_next_power_of_two()
        .ok_or_else(|| TraceqlError::Exec("histogram bucket exceeds u64".into()))?;
    let bucket = f64_from_u64(bucket)?;
    Ok(Some(
        if matches!(
            field.scope,
            Scope::Intrinsic(
                Intrinsic::Duration | Intrinsic::TraceDuration | Intrinsic::EventTimeSinceStart
            )
        ) {
            bucket / 1_000_000_000.0
        } else {
            bucket
        },
    ))
}

// Metric Duration Label
/// Match Go time.Duration.String used by Tempo Static.AsAnyValue.
pub(crate) fn metric_duration_label(nanos: i64) -> String {
    if nanos == 0 {
        return "0s".into();
    }
    let magnitude = nanos.unsigned_abs();
    let sign = if nanos < 0 { "-" } else { "" };
    for unit in [
        DurationUnit {
            bound: 1_000,
            divisor: 1,
            suffix: "ns",
            precision: 0,
        },
        DurationUnit {
            bound: 1_000_000,
            divisor: 1_000,
            suffix: "µs",
            precision: 3,
        },
        DurationUnit {
            bound: 1_000_000_000,
            divisor: 1_000_000,
            suffix: "ms",
            precision: 6,
        },
    ] {
        if magnitude < unit.bound {
            return format!(
                "{sign}{}{suffix}",
                decimal_duration(magnitude, unit.divisor, unit.precision),
                suffix = unit.suffix,
            );
        }
    }
    let hours = magnitude / 3_600_000_000_000;
    let remainder = magnitude % 3_600_000_000_000;
    let minutes = remainder / 60_000_000_000;
    let seconds = decimal_duration(remainder % 60_000_000_000, 1_000_000_000, 9);
    if hours > 0 {
        format!("{sign}{hours}h{minutes}m{seconds}s")
    } else if minutes > 0 {
        format!("{sign}{minutes}m{seconds}s")
    } else {
        format!("{sign}{seconds}s")
    }
}

struct DurationUnit {
    bound: u64,
    divisor: u64,
    suffix: &'static str,
    precision: usize,
}

fn decimal_duration(value: u64, divisor: u64, precision: usize) -> String {
    let integral = value / divisor;
    let remainder = value % divisor;
    if remainder == 0 {
        return integral.to_string();
    }
    let fraction = format!("{remainder:0precision$}");
    format!("{integral}.{}", fraction.trim_end_matches('0'))
}

#[cfg(test)]
mod metric_bucket_tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn incremental_average_stays_finite_when_the_sum_overflows() {
        let mut bucket = MetricBucket::default();
        bucket.record(Some(1e308), None);
        bucket.record(Some(1e308), None);
        assert!(bucket.sum.is_infinite());
        assert!(bucket.average().to_bits() == 1e308_f64.to_bits());
        assert!(bucket.min.unwrap().to_bits() == 1e308_f64.to_bits());
    }
}

#[cfg(test)]
mod metric_exemplars_tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn timestamps_share_two_exemplar_slots_per_stratum_and_end_is_inclusive() {
        let mut buckets = ExemplarBuckets::new(6, 1_000_000_000, 7_000_000_000);
        let decisions = [
            999_000_000,
            1_000_000_000,
            2_999_000_000,
            2_500_000_000,
            3_000_000_000,
            4_999_000_000,
            4_500_000_000,
            5_000_000_000,
            7_000_000_000,
            7_000_000_001,
        ]
        .map(|timestamp| buckets.admit(timestamp));
        assert!(
            decisions
                == [
                    false, true, true, false, true, true, false, true, true, false
                ]
        );
        assert!(!buckets.admit(6_000_000_000));
        let mut few = ExemplarBuckets::new(1, 0, 1_000_000);
        assert!(few.admit(0));
        assert!(!few.admit(1_000_000));
        let mut none = ExemplarBuckets::new(0, 0, 0);
        assert!(!none.admit(0));
    }
}

#[cfg(test)]
mod metric_label_key_tests {
    use assert2::assert;

    use super::{Field, Intrinsic, Scope, metric_label_key};

    #[test]
    fn intrinsic_span_identity_is_canonical_and_does_not_collide_with_attribute_id() {
        assert!(
            metric_label_key(&Field {
                scope: Scope::Intrinsic(Intrinsic::Id),
                key: "id".into()
            }) == "span:id"
        );
        assert!(
            metric_label_key(&Field {
                scope: Scope::Span,
                key: "id".into()
            }) == "span.id"
        );
        assert!(
            metric_label_key(&Field {
                scope: Scope::Both,
                key: "id".into()
            }) == ".id"
        );
    }

    #[test]
    fn scoped_aliases_keep_pinned_unscoped_metric_intrinsic_names() {
        struct AliasCase {
            intrinsic: Intrinsic,
            key: &'static str,
        }

        for case in [
            AliasCase {
                intrinsic: Intrinsic::Name,
                key: "name",
            },
            AliasCase {
                intrinsic: Intrinsic::Duration,
                key: "duration",
            },
            AliasCase {
                intrinsic: Intrinsic::Status,
                key: "status",
            },
            AliasCase {
                intrinsic: Intrinsic::StatusMessage,
                key: "statusMessage",
            },
            AliasCase {
                intrinsic: Intrinsic::Kind,
                key: "kind",
            },
        ] {
            let AliasCase { intrinsic, key } = case;
            assert!(
                metric_label_key(&Field {
                    scope: Scope::Intrinsic(intrinsic),
                    key: format!("span:{key}")
                }) == key
            );
            assert!(
                metric_label_key(&Field {
                    scope: Scope::Span,
                    key: key.into()
                }) == format!("span.{key}")
            );
        }
    }
}

#[cfg(test)]
mod metric_duration_label_tests {
    use super::*;

    #[yare::parameterized(
        zero = { 0, "0s" },
        nanoseconds = { 5, "5ns" },
        fractional_microseconds = { 1_001, "1.001µs" },
        fractional_milliseconds = { 1_500_000, "1.5ms" },
        fractional_seconds = { 1_000_000_001, "1.000000001s" },
        negative = { -61_500_000_000, "-1m1.5s" },
        hours = { 3_600_000_000_000, "1h0m0s" },
        min_i64 = { i64::MIN, "-2562047h47m16.854775808s" },
    )]
    fn duration_labels_preserve_units_fractional_parts_and_sign(nanos: i64, expected: &str) {
        ::core::assert_eq!(metric_duration_label(nanos), expected);
    }
}

// Apply Metric Filter
pub(crate) fn apply_metric_filter(
    series: Vec<TraceMetricSeries>,
    filter: Option<MetricFilter>,
) -> Vec<TraceMetricSeries> {
    let Some(filter) = filter else {
        return series;
    };
    series
        .into_iter()
        .filter_map(|mut series| {
            series
                .points
                .retain(|point| !point.value.is_nan() && metric_filter_passes(point.value, filter));
            series.exemplars.retain(|exemplar| {
                !exemplar.value.is_nan() && metric_filter_passes(exemplar.value, filter)
            });
            if series.points.is_empty() {
                None
            } else {
                Some(series)
            }
        })
        .collect()
}

pub(crate) fn histogram_points(
    buckets: &[MetricBucket],
    start_ns: i64,
    step_ns: i64,
    mut value: impl FnMut(&MetricBucket) -> Result<f64>,
) -> Result<Vec<MetricPoint>> {
    buckets
        .iter()
        .enumerate()
        .map(|indexed_bucket| {
            let idx = indexed_bucket.0;
            let bucket = indexed_bucket.1;
            let ts = start_ns + i64::try_from(idx).unwrap_or(i64::MAX) * step_ns;
            Ok(MetricPoint::new(ts, value(bucket)?))
        })
        .collect()
}

pub(crate) fn histogram_series_for_group(
    labels: MetricLabels,
    buckets: &[MetricBucket],
    start_ns: i64,
    step_ns: i64,
    exemplars: &[TraceMetricExemplar],
    _histogram_buckets: &[Time],
) -> Result<Vec<TraceMetricSeries>> {
    let label_types = labels.types;
    let labels = labels.labels;
    let mut boundaries = buckets
        .iter()
        .flat_map(|bucket| bucket.values.iter().copied())
        .collect::<Vec<_>>();
    boundaries.sort_by(f64::total_cmp);
    boundaries.dedup_by(|lhs, rhs| lhs.to_bits() == rhs.to_bits());
    boundaries
        .into_iter()
        .map(|boundary| {
            let mut labels = labels.clone();
            labels.insert(0, MetricLabel::new("__bucket", quantile_label(boundary)));
            Ok(TraceMetricSeries {
                labels,
                label_types: label_types.clone(),
                points: histogram_points(buckets, start_ns, step_ns, |bucket| {
                    f64_from_usize(
                        bucket
                            .values
                            .iter()
                            .filter(|value| value.to_bits() == boundary.to_bits())
                            .count(),
                    )
                })?,
                exemplars: exemplars.to_owned(),
            })
        })
        .collect()
}

/// Builds a bare `count_over_time()` `MetricPlan`: no grouping, no stages, no
/// sampling, and no `compare()` stage.
pub(crate) fn count_over_time_plan() -> MetricPlan {
    MetricPlan {
        function: MetricFunction::CountOverTime,
        value: None,
        quantiles: Vec::new(),
        by: Vec::new(),
        exemplar_fields: Vec::new(),
        stages: Vec::new(),
        spanset_pipeline: Vec::new(),
        sampling_factor: 1.0,
        spanset_pipeline_had_input: false,
        frontend_labels: false,
        instant: false,
        compare: None,
    }
}

pub(crate) fn rank_limit(pipeline: &Pipeline) -> Result<RankLimit> {
    if matches!(pipeline, Pipeline::TopK(0) | Pipeline::BottomK(0)) {
        return Err(TraceqlError::Plan(
            "metrics rank limit must be positive".into(),
        ));
    }
    rank_stage_limit(pipeline).ok_or_else(|| {
        TraceqlError::Unsupported(format!(
            "traceql metrics: expected topk/bottomk, got {pipeline:?}"
        ))
    })
}

#[derive(Clone, Copy)]
pub(crate) struct MetricsRange {
    pub(crate) scan_start: UnixNano,
    pub(crate) scan_end: UnixNano,
    pub(crate) output_start: UnixNano,
    pub(crate) step: DurationNanos,
    pub(crate) instant: bool,
}

pub(crate) fn usize_from_integer_f64(value: f64) -> Result<usize> {
    // No finiteness clause: `fract()` of an infinity or a NaN is NaN, and
    // `NaN != 0.0` holds, so the fractional test already refuses every
    // non-finite value. A separate `is_finite` test is unreachable.
    if value < 0.0 || value.fract() != 0.0 {
        return Err(TraceqlError::Exec(format!(
            "expected non-negative integer float, got {value}"
        )));
    }
    value
        .to_string()
        .parse()
        .map_err(|e: std::num::ParseIntError| TraceqlError::Exec(e.to_string()))
}
