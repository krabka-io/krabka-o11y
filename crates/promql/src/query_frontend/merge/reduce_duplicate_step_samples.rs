use super::{
    PromqlError, QueryShardReducer, RangeSeriesSample, SampleValue, add_compatible_native_histogram,
};

pub(crate) fn reduce_duplicate_step_samples(
    samples: &mut Vec<RangeSeriesSample>,
    reducer: QueryShardReducer,
) -> Result<(), PromqlError> {
    let mut merged_samples = Vec::<RangeSeriesSample>::with_capacity(samples.len());
    for sample in samples.drain(..) {
        let ts_ms = sample.timestamp_ms;
        let value = sample.value;
        match merged_samples.last_mut() {
            Some(last)
                if last.timestamp_ms == ts_ms && matches!(&last.value, SampleValue::Float(_)) =>
            {
                let SampleValue::Float(last_value) = &mut last.value else {
                    unreachable!()
                };
                if let SampleValue::Float(value) = value {
                    *last_value = match reducer {
                        QueryShardReducer::First => *last_value,
                        QueryShardReducer::Sum => *last_value + value,
                        QueryShardReducer::Min => last_value.min(value),
                        QueryShardReducer::Max => last_value.max(value),
                    };
                }
            }
            Some(last)
                if last.timestamp_ms == ts_ms
                    && reducer == QueryShardReducer::Sum
                    && matches!(&last.value, SampleValue::Histogram(_)) =>
            {
                let SampleValue::Histogram(last_value) = &mut last.value else {
                    unreachable!()
                };
                if let SampleValue::Histogram(value) = value {
                    add_compatible_native_histogram(last_value, &value)?;
                }
            }
            Some(last) if last.timestamp_ms == ts_ms => {}
            _ => merged_samples.push(RangeSeriesSample::new(ts_ms, value)),
        }
    }
    *samples = merged_samples;
    Ok(())
}
