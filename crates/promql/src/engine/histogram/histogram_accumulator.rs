use super::{
    NativeHistogram, PromqlError, Result, add_compatible_native_histogram, combined_reset_hint,
    divide_native_histogram_values, reconcile_native_histogram_layouts,
    scale_native_histogram_values,
};
use crate::engine::range_functions::kahan_sum_inc;

/// Compensated histogram sum with an overflow-safe incremental mean.
#[derive(Clone)]
pub(crate) struct HistogramAccumulator {
    pub(crate) value: NativeHistogram,
    compensation: NativeHistogram,
    count: f64,
    incremental_mean: bool,
}

impl HistogramAccumulator {
    pub(crate) fn new(value: NativeHistogram) -> Self {
        let mut compensation = value.clone();
        compensation.count = 0.0;
        compensation.sum = 0.0;
        compensation.zero_count = 0.0;
        compensation.positive_counts.fill(0.0);
        compensation.negative_counts.fill(0.0);
        Self {
            value,
            compensation,
            count: 1.0,
            incremental_mean: false,
        }
    }

    pub(crate) fn push(&mut self, value: &NativeHistogram, average: bool) -> Result<()> {
        self.count += 1.0;
        if average && !self.incremental_mean {
            let mut trial = self.clone();
            trial.add(value)?;
            if !trial.has_overflow() {
                *self = trial;
                return Ok(());
            }
            self.incremental_mean = true;
            divide_native_histogram_values(&mut self.value, self.count - 1.0);
            divide_native_histogram_values(&mut self.compensation, self.count - 1.0);
        }
        if self.incremental_mean {
            let weight = (self.count - 1.0) / self.count;
            scale_native_histogram_values(&mut self.value, weight);
            scale_native_histogram_values(&mut self.compensation, weight);
            let mut increment = value.clone();
            divide_native_histogram_values(&mut increment, self.count);
            self.add(&increment)
        } else {
            self.add(value)
        }
    }

    fn add(&mut self, value: &NativeHistogram) -> Result<()> {
        let hint = combined_reset_hint(self.value.reset_hint, value.reset_hint);
        let mut aligned = [self.value.clone(), self.compensation.clone(), value.clone()];
        if !reconcile_native_histogram_layouts(&mut aligned) {
            return Err(PromqlError::Unsupported(
                "cannot combine exponential and custom-bucket native histograms".to_string(),
            ));
        }
        let [mut sum, mut compensation, increment] = aligned;
        (sum.count, compensation.count) =
            kahan_sum_inc(increment.count, sum.count, compensation.count);
        (sum.sum, compensation.sum) = kahan_sum_inc(increment.sum, sum.sum, compensation.sum);
        (sum.zero_count, compensation.zero_count) = kahan_sum_inc(
            increment.zero_count,
            sum.zero_count,
            compensation.zero_count,
        );
        for ((sum, compensation), increment) in sum
            .positive_counts
            .iter_mut()
            .zip(&mut compensation.positive_counts)
            .zip(&increment.positive_counts)
            .chain(
                sum.negative_counts
                    .iter_mut()
                    .zip(&mut compensation.negative_counts)
                    .zip(&increment.negative_counts),
            )
        {
            (*sum, *compensation) = kahan_sum_inc(*increment, *sum, *compensation);
        }
        sum.reset_hint = hint;
        self.value = sum;
        self.compensation = compensation;
        Ok(())
    }

    fn has_overflow(&self) -> bool {
        self.value.count.is_infinite()
            || self.value.sum.is_infinite()
            || self.value.zero_count.is_infinite()
            || self
                .value
                .positive_counts
                .iter()
                .chain(&self.value.negative_counts)
                .any(|count| count.is_infinite())
    }

    pub(crate) fn finish(&self, average: bool) -> NativeHistogram {
        let mut value = self.value.clone();
        let mut compensation = self.compensation.clone();
        if average && !self.incremental_mean {
            divide_native_histogram_values(&mut value, self.count);
            divide_native_histogram_values(&mut compensation, self.count);
        }
        if self.count <= 1.0 {
            return value;
        }
        add_compatible_native_histogram(&mut value, &compensation)
            .expect("the compensation shares the accumulator layout");
        value
    }
}

#[cfg(test)]
mod tests {
    use krabka_metrics::{BucketSpan, NativeHistogram, ResetHint};

    use super::HistogramAccumulator;

    fn histogram(sum: f64, count: f64) -> NativeHistogram {
        NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::Unknown,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count,
            sum,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 1,
            }],
            positive_counts: vec![count],
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: None,
            start_timestamp_ms: None,
        }
    }

    #[test]
    fn singleton_preserves_signed_zero_and_bucket_representation() {
        let mut input = histogram(-0.0, -0.0);
        input.zero_count = -0.0;
        for average in [false, true] {
            let actual = HistogramAccumulator::new(input.clone()).finish(average);
            assert2::assert!(actual == input);
            for value in [
                actual.sum,
                actual.count,
                actual.zero_count,
                actual.positive_counts[0],
            ] {
                assert2::assert!(value.to_bits() == (-0.0_f64).to_bits());
            }
        }
    }

    #[test]
    fn cancellation_keeps_small_observations_and_average_avoids_overflow() {
        for average in [false, true] {
            let mut accumulator = HistogramAccumulator::new(histogram(3.1, 1.0));
            for sum in [1e100, -1e100, 1.3] {
                accumulator.push(&histogram(sum, 1.0), average).unwrap();
            }
            let expected = histogram(
                if average { 1.1 } else { 4.4 },
                if average { 1.0 } else { 4.0 },
            );
            assert2::assert!(accumulator.finish(average) == expected);
        }
        let mut accumulator = HistogramAccumulator::new(histogram(1.0, 1e308));
        accumulator.push(&histogram(3.0, 1e308), true).unwrap();
        assert2::assert!(accumulator.finish(true) == histogram(2.0, 1e308));
    }
}
