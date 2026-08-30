use super::{Accumulator, ArrayRef, AsArray, DfResult, Extremum, Float64Type, ScalarValue};

/// Prometheus-faithful NaN-ignoring `min`/`max` accumulator over `Float64`
/// samples.
///
/// `running` holds the seeded extremum after `seen` is set.
#[derive(Debug)]
pub(crate) struct PromExtremumAccumulator {
    pub(crate) extremum: Extremum,
    pub(crate) running: f64,
    pub(crate) seen: bool,
}

impl PromExtremumAccumulator {
    pub(crate) fn new(extremum: Extremum) -> Self {
        Self {
            extremum,
            running: f64::NAN,
            seen: false,
        }
    }

    /// Folds one float sample into the running extremum.
    ///
    /// The first observation seeds the extremum, NaN included. Each later sample
    /// goes through the NaN-ignoring replacement rule.
    pub(crate) fn observe(&mut self, value: f64) {
        if self.seen {
            if self.extremum.should_replace(self.running, value) {
                self.running = value;
            }
        } else {
            self.seen = true;
            self.running = value;
        }
    }
}

impl Accumulator for PromExtremumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        // Single `Float64` input column. Arrow nulls cannot appear on the
        // operator path's `value` column (the leaf always emits a non-null
        // float), but a null is skipped defensively rather than seeding the
        // accumulator with a spurious sample.
        let array = values[0].as_primitive::<Float64Type>();
        for value in array.iter().flatten() {
            self.observe(value);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        // An unseen accumulator (empty group) reports NULL; the planner never
        // emits an empty group, so this only guards against misuse.
        let value = if self.seen { Some(self.running) } else { None };
        Ok(ScalarValue::Float64(value))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        // Serialize the running extremum plus the seen flag so partial
        // aggregates merge correctly. An unseen partition emits NULL/false and
        // contributes nothing on merge.
        let running = if self.seen { Some(self.running) } else { None };
        Ok(vec![
            ScalarValue::Float64(running),
            ScalarValue::Boolean(Some(self.seen)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        // `states[0]` = each partition's running extremum, `states[1]` = its
        // seen flag. Only seen partitions contribute; their running value folds
        // through the same NaN-ignoring rule, so the merge of partial states
        // matches a single-pass scan exactly (including all-NaN -> NaN).
        let running = states[0].as_primitive::<Float64Type>();
        let seen = states[1].as_boolean();
        for (running, seen) in running.iter().zip(seen.iter()) {
            if seen == Some(true) {
                // A seen partition always carries a (possibly NaN) running value.
                self.observe(running.unwrap_or(f64::NAN));
            }
        }
        Ok(())
    }
}
