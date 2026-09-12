use super::Value;

pub(crate) fn apply_metric_selection(value: &mut Value, limit: usize, largest: bool) {
    let result_type = value.pointer("/data/resultType").and_then(Value::as_str);
    let is_vector = result_type == Some("vector");
    let is_matrix = result_type == Some("matrix");
    let Some(series) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    if is_vector {
        series.sort_by(|left, right| {
            let left = sample_value(left.get("value"));
            let right = sample_value(right.get("value"));
            if largest {
                right.total_cmp(&left)
            } else {
                left.total_cmp(&right)
            }
        });
        series.truncate(limit);
        return;
    }
    if !is_matrix {
        return;
    }

    let mut ranked = std::collections::HashMap::<String, Vec<(usize, f64)>>::new();
    for (series_index, item) in series.iter().enumerate() {
        for sample in item
            .get("values")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(timestamp) = sample.as_array().and_then(|parts| parts.first()) else {
                continue;
            };
            ranked
                .entry(timestamp.to_string())
                .or_default()
                .push((series_index, sample_value(Some(sample))));
        }
    }
    let mut selected = std::collections::HashSet::new();
    for (timestamp, candidates) in &mut ranked {
        candidates.sort_by(|left, right| {
            if largest {
                right.1.total_cmp(&left.1)
            } else {
                left.1.total_cmp(&right.1)
            }
        });
        selected.extend(
            candidates
                .iter()
                .take(limit)
                .map(|(series_index, _)| (*series_index, timestamp.clone())),
        );
    }
    for (series_index, item) in series.iter_mut().enumerate() {
        if let Some(samples) = item.get_mut("values").and_then(Value::as_array_mut) {
            samples.retain(|sample| {
                sample
                    .as_array()
                    .and_then(|parts| parts.first())
                    .is_some_and(|timestamp| {
                        selected.contains(&(series_index, timestamp.to_string()))
                    })
            });
        }
    }
    series.retain(|item| {
        item.get("values")
            .and_then(Value::as_array)
            .is_some_and(|samples| !samples.is_empty())
    });
}

fn sample_value(sample: Option<&Value>) -> f64 {
    sample
        .and_then(Value::as_array)
        .and_then(|parts| parts.get(1))
        .and_then(Value::as_str)
        .and_then(|value| value.parse().ok())
        .unwrap_or(f64::NEG_INFINITY)
}
