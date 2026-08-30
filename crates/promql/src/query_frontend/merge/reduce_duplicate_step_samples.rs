use super::{PromqlError, QueryShardReducer, SampleValue, add_compatible_native_histogram};

pub(crate) fn reduce_duplicate_step_samples(
    samples: &mut Vec<(i64, SampleValue)>,
    reducer: QueryShardReducer,
) -> Result<(), PromqlError> {
    let mut merged_samples = Vec::<(i64, SampleValue)>::with_capacity(samples.len());
    for (ts_ms, value) in samples.drain(..) {
        match merged_samples.last_mut() {
            Some((last_ts, SampleValue::Float(last_value))) if *last_ts == ts_ms => {
                if let SampleValue::Float(value) = value {
                    *last_value = match reducer {
                        QueryShardReducer::First => *last_value,
                        QueryShardReducer::Sum => *last_value + value,
                        QueryShardReducer::Min => last_value.min(value),
                        QueryShardReducer::Max => last_value.max(value),
                    };
                }
            }
            Some((last_ts, SampleValue::Histogram(last_value)))
                if *last_ts == ts_ms && reducer == QueryShardReducer::Sum =>
            {
                if let SampleValue::Histogram(value) = value {
                    add_compatible_native_histogram(last_value, &value)?;
                }
            }
            Some((last_ts, _)) if *last_ts == ts_ms => {}
            _ => merged_samples.push((ts_ms, value)),
        }
    }
    *samples = merged_samples;
    Ok(())
}
