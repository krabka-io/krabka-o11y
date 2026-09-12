use super::{
    CounterResetHints, Labels, NativeHistogram, Result, add_compatible_native_histogram,
    kahan_sum_inc,
};

#[allow(clippy::struct_excessive_bools)]
pub(crate) struct AggregateState {
    pub(crate) labels: Labels,
    pub(crate) drop_name: bool,
    pub(crate) count: usize,
    pub(crate) count_f64: f64,
    /// Kahan-compensated running sum for `sum`, which is `sum + sum_comp`.
    pub(crate) sum: f64,
    pub(crate) sum_comp: f64,
    /// Kahan-compensated running mean for `avg`. Prometheus takes the DIRECT
    /// mean -- the compensated sum over the count -- because that is the more
    /// accurate of the two, and falls back to an incremental mean only once the
    /// running sum would overflow to an infinity. `avg_incremental` records that
    /// the fallback has happened; until it does, the answer comes from
    /// `avg_sum`, and after it, from `avg_mean`. Both share `avg_comp`.
    pub(crate) avg_sum: f64,
    pub(crate) avg_mean: f64,
    pub(crate) avg_comp: f64,
    pub(crate) avg_incremental: bool,
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
    /// The counter-reset hints the group's histograms have stated.
    pub(crate) counter_reset_hints: CounterResetHints,
}

impl AggregateState {
    pub(crate) fn new(labels: Labels) -> Self {
        Self {
            labels,
            drop_name: false,
            count: 0,
            count_f64: 0.0,
            sum: 0.0,
            sum_comp: 0.0,
            avg_sum: 0.0,
            avg_mean: 0.0,
            avg_comp: 0.0,
            avg_incremental: false,
            var_mean: 0.0,
            var_mean_comp: 0.0,
            var_aux: 0.0,
            var_aux_comp: 0.0,
            seen_float: false,
            min: f64::NAN,
            max: f64::NAN,
            histogram: None,
            invalid_mixed_sample_type: false,
            counter_reset_hints: CounterResetHints::default(),
        }
    }

    pub(crate) fn push_float(&mut self, value: f64) {
        self.push_observation();
        (self.sum, self.sum_comp) = kahan_sum_inc(value, self.sum, self.sum_comp);

        // `avg` sums directly and divides once at the end, and only switches to
        // an incremental mean when that sum would saturate to an infinity --
        // carrying the mean and its compensation across at the switch. The
        // first sample SEEDS the sum rather than being added to it, so a group
        // whose only sample is an infinity does not read as an overflow.
        if self.count_f64 <= 1.0 {
            self.avg_sum = value;
        } else if !self.avg_incremental {
            let (sum, comp) = kahan_sum_inc(value, self.avg_sum, self.avg_comp);
            if sum.is_infinite() {
                self.avg_incremental = true;
                self.avg_mean = self.avg_sum / (self.count_f64 - 1.0);
                self.avg_comp /= self.count_f64 - 1.0;
            } else {
                self.avg_sum = sum;
                self.avg_comp = comp;
            }
        }
        if self.avg_incremental {
            let weight = (self.count_f64 - 1.0) / self.count_f64;
            (self.avg_mean, self.avg_comp) = kahan_sum_inc(
                value / self.count_f64,
                weight * self.avg_mean,
                weight * self.avg_comp,
            );
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
        self.counter_reset_hints.observe(histogram.reset_hint);
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

    /// Whether the group summed histograms that disagree about a counter reset.
    pub(crate) fn has_counter_reset_collision(&self) -> bool {
        self.counter_reset_hints.collide()
    }

    /// The group's `avg`, from whichever of the two folds is live.
    pub(crate) fn mean(&self) -> f64 {
        if self.avg_incremental {
            return self.avg_mean + self.avg_comp;
        }
        self.avg_sum / self.count_f64 + self.avg_comp / self.count_f64
    }

    pub(crate) fn population_variance(&self) -> f64 {
        // Welford `M2 / n` (the running `var_aux` already accumulates the sum of
        // squared deviations from the running mean), Kahan-corrected.
        (self.var_aux + self.var_aux_comp) / self.count_f64
    }
}
