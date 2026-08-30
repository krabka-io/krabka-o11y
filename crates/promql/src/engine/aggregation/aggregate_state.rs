use super::{Labels, NativeHistogram, Result, add_compatible_native_histogram, kahan_sum_inc};

pub(crate) struct AggregateState {
    pub(crate) labels: Labels,
    pub(crate) count: usize,
    pub(crate) count_f64: f64,
    pub(crate) sum: f64,
    /// Incremental Kahan-compensated mean for `avg`, which is
    /// `avg_mean + avg_comp`. This matches Prometheus. The naive `sum / count`
    /// overflows to +/-Inf for groups with a very large magnitude. The
    /// incremental form stays finite, and once it does saturate it keeps the
    /// same-sign-infinity handling.
    pub(crate) avg_mean: f64,
    pub(crate) avg_comp: f64,
    /// Welford running mean and `M2` accumulators for `stddev`/`stdvar`, each
    /// Kahan-compensated. The naive `E[x^2] - E[x]^2` form has catastrophic
    /// cancellation for groups of large, close values, and it then gives a
    /// negative variance whose `sqrt` is NaN. Welford stays stable and matches
    /// Prometheus.
    pub(crate) var_mean: f64,
    pub(crate) var_mean_comp: f64,
    pub(crate) var_aux: f64,
    pub(crate) var_aux_comp: f64,
    /// Running `min`/`max` over the group's float samples. Prometheus' `min` and
    /// `max` ignore NaN: a group's extremum comes from its non-NaN values, and
    /// the result is NaN only when every sample is NaN. This code mirrors
    /// Prometheus' aggregation loop in `promql/engine.go` exactly. The first
    /// sample seeds the running value, NaN included, and each later sample `f`
    /// replaces the running value when `running {>,<} f` or when `running` is
    /// NaN. So a later non-NaN value always displaces an earlier NaN, and an
    /// all-NaN group keeps NaN. `seen_float` tracks whether the code has taken
    /// the seed.
    pub(crate) seen_float: bool,
    pub(crate) min: f64,
    pub(crate) max: f64,
    pub(crate) histogram: Option<NativeHistogram>,
    pub(crate) invalid_mixed_sample_type: bool,
}

impl AggregateState {
    pub(crate) fn new(labels: Labels) -> Self {
        Self {
            labels,
            count: 0,
            count_f64: 0.0,
            sum: 0.0,
            avg_mean: 0.0,
            avg_comp: 0.0,
            var_mean: 0.0,
            var_mean_comp: 0.0,
            var_aux: 0.0,
            var_aux_comp: 0.0,
            seen_float: false,
            min: f64::NAN,
            max: f64::NAN,
            histogram: None,
            invalid_mixed_sample_type: false,
        }
    }

    pub(crate) fn push_float(&mut self, value: f64) {
        self.push_observation();
        self.sum += value;

        // Incremental Kahan-compensated mean for `avg` (Prometheus' `avg_over`-
        // style fold), keeping the running mean finite past naive-sum overflow.
        // Once the mean is infinite, a same-sign infinity or any finite sample
        // leaves it unchanged (only a flip to the opposite infinity / a NaN moves
        // it), exactly as Prometheus' `avg` aggregation does.
        let keep_infinite_mean = self.avg_mean.is_infinite()
            && ((value.is_infinite() && (value > 0.0) == (self.avg_mean > 0.0))
                || (!value.is_infinite() && !value.is_nan()));
        if !keep_infinite_mean {
            let (mean, comp) = kahan_sum_inc(
                value / self.count_f64 - self.avg_mean / self.count_f64,
                self.avg_mean,
                self.avg_comp,
            );
            self.avg_mean = mean;
            self.avg_comp = comp;
        }

        // Welford + Kahan variance accumulation for `stddev`/`stdvar`.
        let delta = value - (self.var_mean + self.var_mean_comp);
        let (var_mean, var_mean_comp) =
            kahan_sum_inc(delta / self.count_f64, self.var_mean, self.var_mean_comp);
        self.var_mean = var_mean;
        self.var_mean_comp = var_mean_comp;
        let (var_aux, var_aux_comp) = kahan_sum_inc(
            delta * (value - (self.var_mean + self.var_mean_comp)),
            self.var_aux,
            self.var_aux_comp,
        );
        self.var_aux = var_aux;
        self.var_aux_comp = var_aux_comp;

        if self.seen_float {
            // Replace the running extremum when the new sample wins under the
            // float ordering, or when the running value is NaN (so a non-NaN
            // sample displaces a NaN seed). `NaN > _` / `NaN < _` are false, so
            // a NaN sample never displaces an existing non-NaN extremum.
            if self.min > value || self.min.is_nan() {
                self.min = value;
            }
            if self.max < value || self.max.is_nan() {
                self.max = value;
            }
        } else {
            // First sample seeds both extrema (NaN included).
            self.seen_float = true;
            self.min = value;
            self.max = value;
        }
    }

    pub(crate) fn push_observation(&mut self) {
        self.count += 1;
        self.count_f64 += 1.0;
    }

    pub(crate) fn push_histogram(&mut self, histogram: NativeHistogram) -> Result<()> {
        if self.invalid_mixed_sample_type {
            return Ok(());
        }
        if self.count != 0 && self.histogram.is_none() {
            self.mark_invalid_mixed_sample_type();
            return Ok(());
        }
        self.push_observation();
        match &mut self.histogram {
            Some(existing) => add_compatible_native_histogram(existing, &histogram)?,
            None => self.histogram = Some(histogram),
        }
        Ok(())
    }

    pub(crate) fn mark_invalid_mixed_sample_type(&mut self) {
        self.invalid_mixed_sample_type = true;
        self.histogram = None;
    }

    pub(crate) fn has_histogram(&self) -> bool {
        self.histogram.is_some()
    }

    pub(crate) fn population_variance(&self) -> f64 {
        // Welford `M2 / n` (the running `var_aux` already accumulates the sum of
        // squared deviations from the running mean), Kahan-corrected.
        (self.var_aux + self.var_aux_comp) / self.count_f64
    }
}
