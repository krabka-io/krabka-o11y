use super::{BTreeMap, DeltaKey, DeltaState, DeltaHistogramState, Labels, delta_key, NativeHistogram, OtlpError, add_compatible_native_histogram};

/// Stateful accumulator for OTLP delta-temporality sums and histograms.
#[derive(Clone, Debug, Default)]
pub struct DeltaAccumulator {
    pub(crate) sums: BTreeMap<DeltaKey, DeltaState>,
    pub(crate) histograms: BTreeMap<DeltaKey, DeltaHistogramState>,
}

impl DeltaAccumulator {
    pub(crate) fn accumulate_sum(&mut self, labels: &Labels, start_time_unix_nano: u64, delta: f64) -> f64 {
        let key = delta_key(labels);
        let state = self.sums.entry(key).or_default();
        if start_time_unix_nano != 0
            && state.start_time_unix_nano != 0
            && state.start_time_unix_nano != start_time_unix_nano
        {
            state.value = delta;
        } else {
            state.value += delta;
        }
        if start_time_unix_nano != 0 {
            state.start_time_unix_nano = start_time_unix_nano;
        }
        state.value
    }

    pub(crate) fn accumulate_histogram(
        &mut self,
        metric_name: &str,
        labels: &Labels,
        start_time_unix_nano: u64,
        delta: NativeHistogram,
    ) -> Result<NativeHistogram, OtlpError> {
        let key = delta_key(labels);
        let state = self.histograms.entry(key).or_default();
        if start_time_unix_nano != 0
            && state.start_time_unix_nano != 0
            && state.start_time_unix_nano != start_time_unix_nano
        {
            state.value = Some(delta);
        } else if let Some(cumulative) = &mut state.value {
            add_compatible_native_histogram(metric_name, cumulative, &delta)?;
        } else {
            state.value = Some(delta);
        }
        if start_time_unix_nano != 0 {
            state.start_time_unix_nano = start_time_unix_nano;
        }
        state.value.clone().ok_or_else(|| {
            OtlpError::Invalid(metric_name.into(), "missing accumulated histogram".into())
        })
    }
}
