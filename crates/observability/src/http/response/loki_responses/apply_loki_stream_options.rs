use std::collections::HashSet;

use super::{
    LokiDirection, LokiStreamOptions, Value, apply_loki_stream_end_bound,
    apply_loki_stream_interval, apply_loki_stream_limit,
};

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
