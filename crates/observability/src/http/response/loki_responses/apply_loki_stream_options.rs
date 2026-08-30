use super::*;

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
