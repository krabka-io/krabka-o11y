#[allow(clippy::wildcard_imports)]
use super::*;

pub(crate) fn loki_streams_response(streams: BTreeMap<Labels, Vec<[String; 2]>>) -> Value {
    loki_streams_response_with_warnings(streams, &[])
}

pub(crate) fn loki_streams_response_with_warnings(
    streams: BTreeMap<Labels, Vec<[String; 2]>>,
    warnings: &[String],
) -> Value {
    let result = streams
        .into_iter()
        .map(|(stream, values)| {
            json!({
                "stream": stream,
                "values": values,
            })
        })
        .collect::<Vec<_>>();

    let mut value = loki_success_value(json!({
        "resultType": "streams",
        "result": result,
    }));
    if !warnings.is_empty() {
        value["warnings"] = json!(warnings);
    }
    value
}

pub(crate) fn loki_matrix_response(series: FormattedMetricSeries) -> Value {
    loki_matrix_response_with_warnings(series, &[])
}

pub(crate) fn loki_matrix_response_with_warnings(
    series: FormattedMetricSeries,
    warnings: &[String],
) -> Value {
    let result = series
        .into_iter()
        .map(|(metric, values)| {
            json!({
                "metric": metric,
                "values": values
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

pub(crate) fn loki_metric_sample([timestamp_ns, value]: [String; 2]) -> Value {
    json!([unix_ns_string_to_loki_seconds(&timestamp_ns), value])
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

pub(crate) fn loki_vector_response_from_matrix(mut value: Value) -> Value {
    if value.pointer("/data/resultType").and_then(Value::as_str) != Some("matrix") {
        return value;
    }

    value["data"]["resultType"] = json!("vector");
    if let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    {
        for result in results {
            if let Some(values) = result.get_mut("values").and_then(Value::as_array_mut) {
                let value_sample = values.pop().unwrap_or_else(|| json!([]));
                result["value"] = value_sample;
            }
            if let Some(object) = result.as_object_mut() {
                object.remove("values");
            }
        }
    }

    value
}

pub(crate) fn apply_loki_stream_options(
    mut value: Value,
    direction: LokiDirection,
    limit: Option<usize>,
    interval: Option<i64>,
    end_exclusive: Option<i64>,
) -> Value {
    if value.pointer("/data/resultType").and_then(Value::as_str) != Some("streams") {
        return value;
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

    apply_loki_stream_limit(value, limit)
}

pub(crate) fn apply_loki_stream_end_bound(value: &mut Value, end_exclusive: Option<i64>) {
    let Some(end_exclusive) = end_exclusive else {
        return;
    };
    let Some(streams) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };

    for stream in streams.iter_mut() {
        let Some(values) = stream.get_mut("values").and_then(Value::as_array_mut) else {
            continue;
        };
        values.retain(|entry| {
            entry
                .as_array()
                .and_then(|entry| entry.first())
                .and_then(Value::as_str)
                .and_then(|timestamp| timestamp.parse::<i64>().ok())
                .is_none_or(|timestamp| timestamp < end_exclusive)
        });
    }
    streams.retain(|stream| {
        stream
            .get("values")
            .and_then(Value::as_array)
            .is_some_and(|values| !values.is_empty())
    });
}

pub(crate) fn apply_loki_stream_interval(value: &mut Value, interval: Option<i64>) {
    let Some(interval) = interval else {
        return;
    };
    if interval == 0 {
        return;
    }
    let Some(streams) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };

    for stream in streams.iter_mut() {
        let Some(values) = stream.get_mut("values").and_then(Value::as_array_mut) else {
            continue;
        };
        let mut next_timestamp = None;
        values.retain(|entry| {
            let Some(timestamp) = entry
                .as_array()
                .and_then(|entry| entry.first())
                .and_then(Value::as_str)
                .and_then(|timestamp| timestamp.parse::<i64>().ok())
            else {
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
    }
    streams.retain(|stream| {
        stream
            .get("values")
            .and_then(Value::as_array)
            .is_some_and(|values| !values.is_empty())
    });
}

pub(crate) fn apply_loki_stream_limit(mut value: Value, limit: Option<usize>) -> Value {
    let Some(limit) = limit else {
        return value;
    };
    if value.pointer("/data/resultType").and_then(Value::as_str) != Some("streams") {
        return value;
    }

    let Some(streams) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return value;
    };

    let mut remaining = limit;
    for stream in streams.iter_mut() {
        let Some(values) = stream.get_mut("values").and_then(Value::as_array_mut) else {
            continue;
        };
        // No `remaining == 0` short circuit: `truncate(0)` already clears.
        if values.len() > remaining {
            values.truncate(remaining);
            remaining = 0;
        } else {
            remaining -= values.len();
        }
    }
    streams.retain(|stream| {
        stream
            .get("values")
            .and_then(Value::as_array)
            .is_some_and(|values| !values.is_empty())
    });

    value
}

pub(crate) const LOKI_PARQUET_CONTENT_TYPE: &str = "application/vnd.apache.parquet";

pub(crate) fn wants_loki_parquet(headers: &HeaderMap) -> bool {
    headers
        .get(ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| accept.split(',').any(accept_part_allows_loki_parquet))
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

pub(crate) fn accept_parameter_is_zero_quality(parameter: &str) -> bool {
    let Some((name, value)) = parameter.trim().split_once('=') else {
        return false;
    };
    if !name.trim().eq_ignore_ascii_case("q") {
        return false;
    }

    value
        .trim()
        .parse::<f32>()
        .is_ok_and(|quality| quality <= 0.0)
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

    let timestamp_data_type = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    let timestamp_array =
        TimestampNanosecondArray::from(timestamps).with_data_type(timestamp_data_type.clone());
    let labels_array = loki_parquet_label_array(&label_sets)?;
    let line_array = StringArray::from(lines);
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", timestamp_data_type, false),
        Field::new("labels", labels_array.data_type().clone(), false),
        Field::new("line", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(timestamp_array) as ArrayRef,
            Arc::new(labels_array) as ArrayRef,
            Arc::new(line_array) as ArrayRef,
        ],
    )?;
    loki_parquet_batch_response(&batch)
}

#[derive(Clone, Copy)]
pub(crate) enum LokiMetricParquetKind {
    Matrix,
    Vector,
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
                    let (timestamp_ns, value) = loki_parquet_metric_sample(sample, kind)?;
                    timestamps.push(timestamp_ns);
                    label_sets.push(labels.clone());
                    values.push(value);
                }
            }
            LokiMetricParquetKind::Vector => {
                let sample = series
                    .get("value")
                    .ok_or(HttpQueryError::LokiParquet("missing vector value"))?;
                let (timestamp_ns, value) = loki_parquet_metric_sample(sample, kind)?;
                timestamps.push(timestamp_ns);
                label_sets.push(labels.clone());
                values.push(value);
            }
        }
    }

    let timestamp_data_type = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    let timestamp_array =
        TimestampNanosecondArray::from(timestamps).with_data_type(timestamp_data_type.clone());
    let labels_array = loki_parquet_label_array(&label_sets)?;
    let value_array = Float64Array::from(values);
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", timestamp_data_type, false),
        Field::new("labels", labels_array.data_type().clone(), false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(timestamp_array) as ArrayRef,
            Arc::new(labels_array) as ArrayRef,
            Arc::new(value_array) as ArrayRef,
        ],
    )?;
    loki_parquet_batch_response(&batch)
}

pub(crate) fn loki_parquet_metric_sample(
    sample: &Value,
    kind: LokiMetricParquetKind,
) -> Result<(i64, f64), HttpQueryError> {
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
    Ok((timestamp_ns, value))
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

pub(crate) fn loki_parquet_labels(
    labels: Option<&Value>,
    field: &'static str,
) -> Result<Vec<(String, String)>, HttpQueryError> {
    let labels = labels
        .and_then(Value::as_object)
        .ok_or(HttpQueryError::LokiParquet(field))?;
    labels
        .iter()
        .map(|(key, value)| {
            value.as_str().map_or_else(
                || Err(HttpQueryError::LokiParquet("label value is not a string")),
                |value| Ok((key.clone(), value.to_string())),
            )
        })
        .collect()
}
