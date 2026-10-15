use datafusion::arrow::array::Array as _;
use num_traits::FromPrimitive as _;

use crate::{
    ACCEPT, Arc, ArrayRef, BTreeMap, DataType, Duration, Field, Float64Array,
    FormattedMetricSeries, HeaderMap, HttpQueryError, Labels, LokiDirection, LokiStreamOptions,
    MetricValue, RecordBatch, Response, Schema, StringArray, TimeUnit, TimestampNanosecondArray,
    Value, json, loki_parquet_batch_response, loki_parquet_label_array, loki_success_value,
    parse_metric_sample_value, sort_loki_stream_values,
};

struct LokiParquetColumns {
    timestamp_data_type: DataType,
    timestamp_array: TimestampNanosecondArray,
    labels_array: datafusion::arrow::array::MapArray,
}

fn loki_parquet_columns(
    timestamps: Vec<i64>,
    label_sets: &[Vec<krabka_blockstore::LabelPair>],
) -> Result<LokiParquetColumns, HttpQueryError> {
    let timestamp_data_type = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    let timestamp_array =
        TimestampNanosecondArray::from(timestamps).with_data_type(timestamp_data_type.clone());
    let labels_array = loki_parquet_label_array(label_sets)?;
    Ok(LokiParquetColumns {
        timestamp_data_type,
        timestamp_array,
        labels_array,
    })
}

fn loki_parquet_value_response(
    columns: LokiParquetColumns,
    value_column_name: &str,
    value_array: ArrayRef,
) -> Result<Response, HttpQueryError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", columns.timestamp_data_type, false),
        Field::new("labels", columns.labels_array.data_type().clone(), false),
        Field::new(value_column_name, value_array.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(columns.timestamp_array) as ArrayRef,
            Arc::new(columns.labels_array) as ArrayRef,
            value_array,
        ],
    )?;
    loki_parquet_batch_response(&batch)
}


fn update_loki_stream_values(value: &mut Value, mut update_values: impl FnMut(&mut Vec<Value>)) {
    let Some(streams) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };

    for stream in streams.iter_mut() {
        if let Some(values) = stream.get_mut("values").and_then(Value::as_array_mut) {
            update_values(values);
        }
    }

    streams.retain(|stream| {
        stream
            .get("values")
            .and_then(Value::as_array)
            .is_some_and(|values| !values.is_empty())
    });
}

fn loki_stream_entry_timestamp(entry: &Value) -> Option<i64> {
    entry
        .as_array()
        .and_then(|entry| entry.first())
        .and_then(Value::as_str)
        .and_then(|timestamp| timestamp.parse::<i64>().ok())
}

/// The one encoding flag Krabka acts on rather than merely echoing.
pub(crate) const CATEGORIZE_LABELS_ENCODING_FLAG: &str = "categorize-labels";

pub(crate) const LOKI_PARQUET_CONTENT_TYPE: &str = "application/vnd.apache.parquet";

/// The header a request uses to ask for a non-default `streams` encoding.
pub(crate) const LOKI_RESPONSE_ENCODING_FLAGS_HEADER: &str = "X-Loki-Response-Encoding-Flags";

/// Echoes the request's encoding flags into a tail frame.
///
/// A tail frame is not a query response, so the flags sit beside `streams`
/// rather than under a `data` object.
pub(crate) fn add_loki_tail_encoding_flags(frame: &mut Value, flags: &[String]) {
    if flags.is_empty() {
        return;
    }
    frame["encodingFlags"] = json!(flags);
}

pub(crate) fn apply_loki_stream_end_bound(value: &mut Value, end_exclusive: Option<i64>) {
    let Some(end_exclusive) = end_exclusive else {
        return;
    };
    update_loki_stream_values(value, |values| {
        values.retain(|entry| {
            loki_stream_entry_timestamp(entry).is_none_or(|timestamp| timestamp < end_exclusive)
        });
    });
}

pub(crate) fn loki_matrix_response(series: FormattedMetricSeries) -> Value {
    loki_matrix_response_with_warnings(series, &[])
}

pub(crate) fn loki_metric_sample([timestamp_ns, value]: [String; 2]) -> Value {
    json!([unix_ns_string_to_loki_seconds(&timestamp_ns), value])
}

pub(crate) fn loki_streams_response(
    streams: BTreeMap<Labels, Vec<LokiStreamEntry>>,
    encoding: LokiStreamEncoding,
) -> Value {
    loki_streams_response_with_warnings(streams, &[], encoding)
}

pub(crate) fn unix_ns_string_to_loki_seconds(timestamp_ns: &str) -> Value {
    let timestamp_ns = timestamp_ns.parse::<u64>().unwrap_or_default();
    let seconds = timestamp_ns / 1_000_000_000;
    let nanos = timestamp_ns % 1_000_000_000;
    if nanos == 0 {
        json!(seconds)
    } else {
        json!(Duration::from_nanos(timestamp_ns).as_secs_f64())
    }
}

pub(crate) fn wants_loki_parquet(headers: &HeaderMap) -> bool {
    headers
        .get(ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| accept.split(',').any(accept_part_allows_loki_parquet))
}

pub(crate) fn accept_parameter_is_zero_quality(parameter: &str) -> bool {
    let Some(parts) = parameter.trim().split_once('=') else {
        return false;
    };
    if !parts.0.trim().eq_ignore_ascii_case("q") {
        return false;
    }

    parts
        .1
        .trim()
        .parse::<f32>()
        .is_ok_and(|quality| quality <= 0.0)
}

pub(crate) fn accept_part_allows_loki_parquet(part: &str) -> bool {
    let mut pieces = part.trim().split(';');
    let Some(mime) = pieces.next() else {
        return false;
    };
    if !mime.trim().eq_ignore_ascii_case(LOKI_PARQUET_CONTENT_TYPE) {
        return false;
    }

    !pieces.any(accept_parameter_is_zero_quality)
}

pub(crate) fn apply_loki_stream_interval(value: &mut Value, interval: Option<i64>) {
    let Some(interval) = interval else {
        return;
    };
    if interval == 0 {
        return;
    }
    update_loki_stream_values(value, |values| {
        let mut next_timestamp = None;
        values.retain(|entry| {
            let Some(timestamp) = loki_stream_entry_timestamp(entry) else {
                return true;
            };
            match next_timestamp {
                Some(next) if timestamp < next => false,
                _ => {
                    next_timestamp = timestamp.checked_add(interval);
                    true
                }
            }
        });
    });
}

/// The encoding flags a request asked for, in the order it named them.
///
/// Loki splits the header on commas and neither trims nor lowercases the
/// pieces, and it echoes them back exactly as given -- including a flag it does
/// not know. Only the first header line counts when a request repeats the
/// header. An absent or empty header asks for nothing at all.
pub(crate) fn loki_encoding_flags(headers: &HeaderMap) -> Vec<String> {
    let Some(header) = headers
        .get(LOKI_RESPONSE_ENCODING_FLAGS_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|header| !header.is_empty())
    else {
        return Vec::new();
    };
    header.split(',').map(str::to_string).collect()
}

pub(crate) fn loki_matrix_response_with_warnings(
    series: FormattedMetricSeries,
    warnings: &[String],
) -> Value {
    let result = series
        .into_iter()
        .map(|series| {
            json!({
                "metric": series.labels,
                "values": series.values
                    .into_iter()
                    .map(loki_metric_sample)
                    .collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();

    let mut value = loki_success_value(json!({
        "resultType": "matrix",
        "result": result,
    }));
    if !warnings.is_empty() {
        value["warnings"] = json!(warnings);
    }
    value
}

pub(crate) fn loki_parquet_labels(
    labels: Option<&Value>,
    field: &'static str,
) -> Result<Vec<krabka_blockstore::LabelPair>, HttpQueryError> {
    let labels = labels
        .and_then(Value::as_object)
        .ok_or(HttpQueryError::LokiParquet(field))?;
    labels
        .iter()
        .map(|entry| {
            let key = entry.0;
            let value = entry.1;
            value.as_str().map_or_else(
                || Err(HttpQueryError::LokiParquet("label value is not a string")),
                |value| Ok(krabka_blockstore::LabelPair::new(key.clone(), value)),
            )
        })
        .collect()
}

pub(crate) fn loki_parquet_metric_timestamp_ns(
    value: &Value,
    kind: LokiMetricParquetKind,
) -> Result<i64, HttpQueryError> {
    if matches!(kind, LokiMetricParquetKind::Vector)
        && let Some(timestamp_ns) = value.as_i64()
    {
        return Ok(timestamp_ns);
    }

    if let Some(seconds) = value.as_i64() {
        return seconds
            .checked_mul(1_000_000_000)
            .ok_or(HttpQueryError::LokiParquet(
                "metric timestamp is out of range",
            ));
    }
    let seconds = value.as_f64().ok_or(HttpQueryError::LokiParquet(
        "metric timestamp is not numeric",
    ))?;
    let timestamp_ns = (seconds * 1_000_000_000.0).round();
    i64::from_f64(timestamp_ns).ok_or(HttpQueryError::LokiParquet(
        "metric timestamp is out of range",
    ))
}

pub(crate) fn loki_parquet_response(value: &Value) -> Result<Response, HttpQueryError> {
    match value.pointer("/data/resultType").and_then(Value::as_str) {
        Some("streams") => loki_streams_parquet_response(value),
        Some("matrix") => loki_metrics_parquet_response(value, LokiMetricParquetKind::Matrix),
        Some("vector") => loki_metrics_parquet_response(value, LokiMetricParquetKind::Vector),
        _ => Err(HttpQueryError::LokiParquet(
            "only stream and metric query results can be encoded as parquet",
        )),
    }
}

/// The `streams` encoding a request asked for.
///
/// Loki matches the flag exactly: `CATEGORIZE-LABELS` is echoed back but
/// changes nothing.
pub(crate) fn loki_stream_encoding_for_headers(headers: &HeaderMap) -> LokiStreamEncoding {
    if loki_encoding_flags(headers)
        .iter()
        .any(|flag| flag == CATEGORIZE_LABELS_ENCODING_FLAG)
    {
        return LokiStreamEncoding::CategorizeLabels;
    }
    LokiStreamEncoding::Folded
}

pub(crate) fn loki_streams_response_with_warnings(
    streams: BTreeMap<Labels, Vec<LokiStreamEntry>>,
    warnings: &[String],
    encoding: LokiStreamEncoding,
) -> Value {
    let mut value = json!({
        "status": "success",
        "data": {"resultType": "streams"},
    });
    value["data"]["result"] = Value::Array(loki_stream_results(streams, encoding));
    if !warnings.is_empty() {
        value["warnings"] = json!(warnings);
    }
    value
}

/// Echoes the request's encoding flags into a `streams` response.
///
/// Loki reports them from the `streams` marshaller alone: a `matrix` or
/// `vector` answer to a request that carried the header comes back without
/// them.
///
/// The flags go in front of `result`, and the position is part of the
/// contract rather than a matter of taste. `serde_json` keeps insertion order
/// here, and Grafana's Loki datasource decodes the body in one pass: it reads
/// `encodingFlags` to learn that an entry is three elements long rather than
/// two. A flag written after `result` therefore arrives too late, and the
/// datasource fails the whole query with `ReadArray: expect [ or , or ] or n,
/// but found {`. Loki writes `resultType`, then `encodingFlags`, then
/// `result`, so this does the same.
pub(crate) fn add_loki_encoding_flags(value: &mut Value, flags: &[String]) {
    if flags.is_empty()
        || value.pointer("/data/resultType").and_then(Value::as_str) != Some("streams")
    {
        return;
    }
    let Some(data) = value.get_mut("data").and_then(Value::as_object_mut) else {
        return;
    };
    let mut ordered = serde_json::Map::new();
    for entry in std::mem::take(data) {
        let name = entry.0;
        let member = entry.1;
        let is_result_type = name == "resultType";
        ordered.insert(name, member);
        if is_result_type {
            ordered.insert("encodingFlags".to_string(), json!(flags));
        }
    }
    *data = ordered;
}

pub(crate) fn apply_loki_stream_limit(
    mut value: Value,
    direction: LokiDirection,
    limit: Option<usize>,
) -> Value {
    crate::query_frontend::apply_global_stream_limit(&mut value, direction, limit);
    value
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use serde_json::json;

    use super::*;

    #[test]
    fn limit_selects_entries_by_time_across_streams_and_preserves_metadata() {
        let response = json!({"status": "success", "data": {
            "resultType": "streams", "result": [
                {"stream": {"app": "a"}, "values": [
                    ["10", "a10", {"structuredMetadata": {"source": "first"}}],
                    ["30", "a30", {"parsed": {"level": "error"}}]
                ]},
                {"stream": {"app": "b"}, "values": [
                    ["20", "b20"], ["40", "b40"]
                ]}
            ]
        }});
        let mut expected = response.clone();
        expected["data"]["result"][1]["values"] = json!([["20", "b20"]]);
        assert!(
            apply_loki_stream_limit(response.clone(), LokiDirection::Forward, Some(3)) == expected
        );

        let mut backwards = response;
        for stream in backwards["data"]["result"].as_array_mut().unwrap() {
            stream["values"].as_array_mut().unwrap().reverse();
        }
        let mut expected = backwards.clone();
        expected["data"]["result"][0]["values"] =
            json!([["30", "a30", {"parsed": {"level": "error"}}]]);
        assert!(apply_loki_stream_limit(backwards, LokiDirection::Backward, Some(3)) == expected);
    }

    #[test]
    fn equal_timestamp_ties_keep_label_order_in_both_directions() {
        let response = json!({"data": {"resultType": "streams", "result": [
            {"stream": {"app": "a"}, "values": [["10", "a"]]},
            {"stream": {"app": "b"}, "values": [["10", "b"]]}
        ]}});
        let expected = json!({"data": {"resultType": "streams", "result": [
            {"stream": {"app": "a"}, "values": [["10", "a"]]}
        ]}});
        for direction in [LokiDirection::Forward, LokiDirection::Backward] {
            assert!(apply_loki_stream_limit(response.clone(), direction, Some(1)) == expected);
            assert!(apply_loki_stream_limit(response.clone(), direction, None) == response);
            let empty = apply_loki_stream_limit(response.clone(), direction, Some(0));
            assert!(empty["data"]["result"] == json!([]));
        }
        let metrics = json!({"data": {"resultType": "matrix", "result": [
            {"metric": {"app": "a"}, "values": [[10, "2"]]}
        ]}});
        assert!(
            apply_loki_stream_limit(metrics.clone(), LokiDirection::Backward, Some(0)) == metrics
        );
    }
}

use std::collections::HashSet;


/// Applies a log query's `options` to a `streams` response, keeping only
/// entries before `end_exclusive` when it is set.
pub(crate) fn apply_loki_stream_options(
    mut value: Value,
    options: LokiStreamOptions,
    end_exclusive: Option<i64>,
) -> Value {
    let LokiStreamOptions {
        direction,
        limit,
        interval,
    } = options;
    if value.pointer("/data/resultType").and_then(Value::as_str) != Some("streams") {
        return value;
    }

    // Hot/cold overlap must not consume the limit before the frontend merges.
    if limit.is_some()
        && let Some(streams) = value
            .pointer_mut("/data/result")
            .and_then(Value::as_array_mut)
    {
        for stream in streams {
            if let Some(values) = stream.get_mut("values").and_then(Value::as_array_mut) {
                // The full entry preserves distinct categorized metadata at one timestamp.
                let mut keep = {
                    let mut seen = HashSet::new();
                    values
                        .iter()
                        .map(|entry| seen.insert(entry))
                        .collect::<Vec<_>>()
                }
                .into_iter();
                values.retain(|_| keep.next().expect("each entry has a retention flag"));
            }
        }
    }

    apply_loki_stream_end_bound(&mut value, end_exclusive);
    apply_loki_stream_interval(&mut value, interval);

    if matches!(direction, LokiDirection::Backward)
        && let Some(streams) = value
            .pointer_mut("/data/result")
            .and_then(Value::as_array_mut)
    {
        for stream in streams {
            if let Some(values) = stream.get_mut("values").and_then(Value::as_array_mut) {
                values.reverse();
            }
        }
    }

    apply_loki_stream_limit(value, direction, limit)
}

/// Regroups folded streams into the `categorize-labels` encoding.
///
/// A stream arrives keyed by the labels the default encoding shows, which
/// already has each entry's structured metadata and parsed labels folded in.
/// Lifting those back out leaves the stream's own labels, and the streams that
/// were split only by a metadata value become one stream again -- which is what
/// Loki returns under this encoding.
pub(crate) fn categorized_loki_stream_results(
    streams: BTreeMap<Labels, Vec<LokiStreamEntry>>,
) -> Vec<Value> {
    let mut categorized: BTreeMap<Labels, Vec<LokiStreamEntry>> = BTreeMap::new();
    for stream_entry in streams {
        for entry in stream_entry.1 {
            let mut stream = stream_entry.0.clone();
            for name in entry.categorized_label_names() {
                stream.remove(name);
            }
            categorized.entry(stream).or_default().push(entry);
        }
    }
    // Merging two folded streams interleaves their entries, so the order the
    // scan established has to be re-established over the merged run.
    sort_loki_stream_values(&mut categorized);

    categorized
        .into_iter()
        .map(|categorized_entry| {
            json!({
                "stream": categorized_entry.0,
                "values": categorized_entry.1
                    .iter()
                    .map(LokiStreamEntry::categorized_value)
                    .collect::<Vec<_>>(),
            })
        })
        .collect()
}

#[derive(Clone, Copy)]
pub(crate) enum LokiMetricParquetKind {
    Matrix,
    Vector,
}

fn append_metric_sample(
    sample: &Value,
    kind: LokiMetricParquetKind,
    labels: &[krabka_blockstore::LabelPair],
    timestamps: &mut Vec<i64>,
    label_sets: &mut Vec<Vec<krabka_blockstore::LabelPair>>,
    values: &mut Vec<f64>,
) -> Result<(), HttpQueryError> {
    let point = loki_parquet_metric_sample(sample, kind)?;
    timestamps.push(point.timestamp_ns);
    label_sets.push(labels.to_vec());
    values.push(point.value);
    Ok(())
}

pub(crate) fn loki_metrics_parquet_response(
    value: &Value,
    kind: LokiMetricParquetKind,
) -> Result<Response, HttpQueryError> {
    let results = value
        .pointer("/data/result")
        .and_then(Value::as_array)
        .ok_or(HttpQueryError::LokiParquet("missing metric result array"))?;
    let mut timestamps = Vec::new();
    let mut label_sets = Vec::new();
    let mut values = Vec::new();
    for series in results {
        let labels = loki_parquet_labels(series.get("metric"), "metric labels")?;
        match kind {
            LokiMetricParquetKind::Matrix => {
                let samples = series
                    .get("values")
                    .and_then(Value::as_array)
                    .ok_or(HttpQueryError::LokiParquet("missing matrix values array"))?;
                for sample in samples {
                    append_metric_sample(
                        sample,
                        kind,
                        &labels,
                        &mut timestamps,
                        &mut label_sets,
                        &mut values,
                    )?;
                }
            }
            LokiMetricParquetKind::Vector => {
                let sample = series
                    .get("value")
                    .ok_or(HttpQueryError::LokiParquet("missing vector value"))?;
                append_metric_sample(
                    sample,
                    kind,
                    &labels,
                    &mut timestamps,
                    &mut label_sets,
                    &mut values,
                )?;
            }
        }
    }

    let columns = loki_parquet_columns(timestamps, &label_sets)?;
    loki_parquet_value_response(columns, "value", Arc::new(Float64Array::from(values)))
}

pub(crate) struct LokiParquetMetricSample {
    pub(crate) timestamp_ns: i64,
    pub(crate) value: f64,
}

pub(crate) fn loki_parquet_metric_sample(
    sample: &Value,
    kind: LokiMetricParquetKind,
) -> Result<LokiParquetMetricSample, HttpQueryError> {
    let sample = sample
        .as_array()
        .ok_or(HttpQueryError::LokiParquet("metric sample is not an array"))?;
    let timestamp_ns = loki_parquet_metric_timestamp_ns(
        sample
            .first()
            .ok_or(HttpQueryError::LokiParquet("missing metric timestamp"))?,
        kind,
    )?;
    let value = sample
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
        .and_then(MetricValue::to_f64)
        .ok_or(HttpQueryError::LokiParquet("metric value is not numeric"))?;
    Ok(LokiParquetMetricSample {
        timestamp_ns,
        value,
    })
}

/// Which JSON encoding a Loki `streams` response uses for its entries.
///
/// The wire choice is Loki's, and a request makes it with the
/// `X-Loki-Response-Encoding-Flags` header.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum LokiStreamEncoding {
    /// Loki's default: an entry's structured metadata and its parsed labels are
    /// folded into the stream's label map, and every entry stays two elements
    /// long.
    #[default]
    Folded,
    /// What `X-Loki-Response-Encoding-Flags: categorize-labels` asks for: the
    /// stream keeps only its own labels, and each entry carries the rest in a
    /// third element, bucketed by where it came from.
    CategorizeLabels,
}

use serde::{Serialize, Serializer, ser::SerializeSeq as _};


/// One entry of a Loki `streams` result, with its labels kept apart by origin.
///
/// Loki's default JSON encoding serialises an entry as the two-element array
/// `["<ns>", "<line>"]` and folds everything the entry carried -- its
/// structured metadata, and any label a parser or `label_format` stage
/// produced -- into the stream's label map. One pushed stream therefore comes
/// back as one stream per distinct metadata value.
///
/// `X-Loki-Response-Encoding-Flags: categorize-labels` asks for the other
/// encoding: the stream keeps only its own labels, and the entry grows a third
/// element that sorts the rest into buckets,
/// `{"structuredMetadata": {...}, "parsed": {...}}`. An empty bucket is
/// omitted, and an entry that has nothing to categorise still gets `{}`.
///
/// The two encodings are the same data grouped differently, so an entry holds
/// both buckets whichever encoding the request asked for, and the response
/// builder folds or splits accordingly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LokiStreamEntry {
    pub(crate) timestamp_ns: LokiTimestamp,
    pub(crate) line: String,
    pub(crate) source_labels: Labels,
    pub(crate) structured_metadata: Labels,
    pub(crate) parsed: Labels,
}

impl LokiStreamEntry {
    /// Builds an entry from a line and the two label buckets behind it.
    pub(crate) fn new(
        timestamp_ns: i64,
        line: String,
        structured_metadata: Labels,
        parsed: Labels,
    ) -> Self {
        Self {
            timestamp_ns: timestamp_ns.into(),
            line,
            source_labels: Labels::new(),
            structured_metadata,
            parsed,
        }
    }

    /// The entry's timestamp, or `None` when it is not a decimal integer.
    pub(crate) fn parsed_timestamp_ns(&self) -> Option<i64> {
        self.timestamp_ns.parsed()
    }

    /// The entry under the `categorize-labels` encoding: three elements, the
    /// last one an envelope that names each bucket it has anything to put in.
    pub(crate) fn categorized_value(&self) -> Value {
        let mut envelope = json!({});
        if !self.structured_metadata.is_empty() {
            envelope["structuredMetadata"] = json!(self.structured_metadata);
        }
        if !self.parsed.is_empty() {
            envelope["parsed"] = json!(self.parsed);
        }
        json!([self.timestamp_ns, self.line, envelope])
    }

    /// The labels this entry carries in a bucket of its own, which the default
    /// encoding folds into the stream and `categorize-labels` keeps out of it.
    pub(crate) fn categorized_label_names(&self) -> impl Iterator<Item = &String> {
        self.structured_metadata.keys().chain(self.parsed.keys())
    }
}

impl Serialize for LokiStreamEntry {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut entry = serializer.serialize_seq(Some(2))?;
        entry.serialize_element(&self.timestamp_ns)?;
        entry.serialize_element(&self.line)?;
        entry.end()
    }
}

/// The `result` array of a `streams` response, in the requested encoding.
pub(crate) fn loki_stream_results(
    streams: BTreeMap<Labels, Vec<LokiStreamEntry>>,
    encoding: LokiStreamEncoding,
) -> Vec<Value> {
    match encoding {
        LokiStreamEncoding::Folded => streams
            .into_iter()
            .map(|entry| {
                let stream = entry.0;
                let values = entry.1;
                let mut result = json!({"stream": stream});
                result["values"] = Value::Array(
                    values
                        .into_iter()
                        .map(|entry| {
                            Value::Array(vec![
                                Value::String(entry.timestamp_ns.into()),
                                Value::String(entry.line),
                            ])
                        })
                        .collect(),
                );
                result
            })
            .collect(),
        LokiStreamEncoding::CategorizeLabels => categorized_loki_stream_results(streams),
    }
}

pub(crate) fn loki_streams_parquet_response(value: &Value) -> Result<Response, HttpQueryError> {
    let results = value
        .pointer("/data/result")
        .and_then(Value::as_array)
        .ok_or(HttpQueryError::LokiParquet("missing stream result array"))?;
    let mut timestamps = Vec::new();
    let mut label_sets = Vec::new();
    let mut lines = Vec::new();
    for stream in results {
        let labels = loki_parquet_labels(stream.get("stream"), "stream labels")?;
        let values = stream
            .get("values")
            .and_then(Value::as_array)
            .ok_or(HttpQueryError::LokiParquet("missing stream values array"))?;
        for entry in values {
            let entry = entry
                .as_array()
                .ok_or(HttpQueryError::LokiParquet("stream value is not an array"))?;
            let timestamp = entry
                .first()
                .and_then(Value::as_str)
                .ok_or(HttpQueryError::LokiParquet(
                    "stream timestamp is not a string",
                ))?
                .parse::<i64>()
                .map_err(|_| HttpQueryError::LokiParquet("stream timestamp is not an integer"))?;
            let line = entry
                .get(1)
                .and_then(Value::as_str)
                .ok_or(HttpQueryError::LokiParquet("stream line is not a string"))?;
            timestamps.push(timestamp);
            label_sets.push(labels.clone());
            lines.push(line.to_string());
        }
    }

    let columns = loki_parquet_columns(timestamps, &label_sets)?;
    loki_parquet_value_response(columns, "line", Arc::new(StringArray::from(lines)))
}

use std::fmt::{self, Display, Formatter};


/// A numeric timestamp until encoding, or an unchanged noncanonical string.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LokiTimestamp(TimestampValue);

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum TimestampValue {
    Nanoseconds(i64),
    Text(String),
}

impl LokiTimestamp {
    /// Nanoseconds, or `None` if the retained text cannot be parsed.
    pub fn parsed(&self) -> Option<i64> {
        match &self.0 {
            TimestampValue::Nanoseconds(value) => Some(*value),
            TimestampValue::Text(value) => value.parse().ok(),
        }
    }
}

impl From<i64> for LokiTimestamp {
    fn from(value: i64) -> Self {
        Self(TimestampValue::Nanoseconds(value))
    }
}

impl From<String> for LokiTimestamp {
    fn from(value: String) -> Self {
        // Only canonical strings share the numeric identity. Preserve spelling
        // for malformed and noncanonical timestamps and their deduplication.
        if let Ok(timestamp) = value.parse::<i64>()
            && timestamp.to_string() == value
        {
            return Self::from(timestamp);
        }
        Self(TimestampValue::Text(value))
    }
}

impl From<&str> for LokiTimestamp {
    fn from(value: &str) -> Self {
        Self::from(value.to_owned())
    }
}

impl From<LokiTimestamp> for String {
    fn from(value: LokiTimestamp) -> Self {
        match value.0 {
            TimestampValue::Nanoseconds(value) => value.to_string(),
            TimestampValue::Text(value) => value,
        }
    }
}

impl Display for LokiTimestamp {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match &self.0 {
            TimestampValue::Nanoseconds(value) => value.fmt(formatter),
            TimestampValue::Text(value) => value.fmt(formatter),
        }
    }
}

impl Serialize for LokiTimestamp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.0 {
            TimestampValue::Nanoseconds(value) => serializer.collect_str(value),
            TimestampValue::Text(value) => serializer.serialize_str(value),
        }
    }
}

#[cfg(test)]
#[path = "loki_responses/loki_timestamp/tests.rs"]
mod loki_timestamp_tests;

pub(crate) fn loki_vector_response_from_matrix(mut value: Value) -> Value {
    if value.pointer("/data/resultType").and_then(Value::as_str) != Some("matrix") {
        return value;
    }

    value["data"]["resultType"] = json!("vector");
    if let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    {
        results.retain_mut(|result| {
            if let Some(values) = result.get_mut("values").and_then(Value::as_array_mut) {
                let Some(value_sample) = values.pop() else {
                    return false;
                };
                result["value"] = value_sample;
            }
            if let Some(object) = result.as_object_mut() {
                object.remove("values");
            }
            true
        });
    }

    value
}

#[cfg(test)]
mod metadata_tests {
    use super::*;

    #[test]
    fn an_empty_window_is_absent_and_a_real_zero_sample_is_preserved() {
        let response = json!({"status":"success", "data": {
            "resultType":"matrix", "stats":{"summary":{"totalLinesProcessed":7}},
            "result":[
                {"metric":{"level":"stale"}, "values":[]},
                {"metric":{"level":"zero"}, "values":[[5,"0"]]},
                {"metric":{"level":"live"}, "values":[[4,"1"],[5,"2"]]}
            ]
        }});
        assert2::assert!(
            loki_vector_response_from_matrix(response)
                == json!({
                    "status":"success", "data": {
                        "resultType":"vector", "stats":{"summary":{"totalLinesProcessed":7}},
                        "result":[
                            {"metric":{"level":"zero"}, "value":[5,"0"]},
                            {"metric":{"level":"live"}, "value":[5,"2"]}
                        ]
                    }
                })
        );
    }
}
