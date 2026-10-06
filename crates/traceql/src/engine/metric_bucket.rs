use super::{Result, TraceMetricExemplar, f64_from_u64, f64_from_usize, usize_from_integer_f64};

#[derive(Clone, Default)]
pub(crate) struct MetricBucket {
    pub(crate) count: u64,
    pub(crate) sum: f64,
    sum_compensation: f64,
    mean: Option<f64>,
    mean_compensation: f64,
    pub(crate) min: Option<f64>,
    pub(crate) max: Option<f64>,
    pub(crate) values: Vec<f64>,
    pub(crate) exemplars: Vec<TraceMetricExemplar>,
}

impl MetricBucket {
    pub(crate) fn record(&mut self, value: Option<f64>, exemplar: Option<TraceMetricExemplar>) {
        self.count += 1;
        if let Some(exemplar) = exemplar
            && self.exemplars.is_empty()
        {
            self.exemplars.push(exemplar);
        }
        let Some(value) = value else {
            return;
        };
        let (sum, compensation) = kahan_increment(self.sum, value - self.sum_compensation, 0.0);
        self.sum = sum;
        self.sum_compensation = compensation;
        if let Some(mean) = self.mean {
            let count =
                f64_from_u64(self.count).expect("u64 values have finite f64 representations");
            let increment = value / count - mean / count;
            if !(mean.is_infinite()
                && (increment.is_finite()
                    || (increment.is_infinite()
                        && mean.is_sign_positive() == increment.is_sign_positive())))
            {
                let (mean, compensation) = kahan_increment(mean, increment, self.mean_compensation);
                self.mean = Some(mean);
                self.mean_compensation = compensation;
            }
        } else {
            self.mean = Some(value);
        }
        self.min = Some(self.min.map_or(value, |min| min.min(value)));
        self.max = Some(self.max.map_or(value, |max| max.max(value)));
        self.values.push(value);
    }

    pub(crate) fn average(&self) -> f64 {
        if self.count == 0 {
            f64::NAN
        } else {
            self.mean.unwrap_or(f64::NAN) + self.mean_compensation
        }
    }

    pub(crate) fn quantile(&self, quantile: f64) -> Result<f64> {
        if self.values.is_empty() {
            return Ok(0.0);
        }
        let mut values = self.values.clone();
        values.sort_by(f64::total_cmp);
        if values.len() == 1 {
            return Ok(values[0]);
        }
        let mut wanted =
            usize_from_integer_f64((quantile * f64_from_usize(values.len())?).ceil())?.max(1);
        let mut index = 0;
        let mut previous: Option<f64> = None;
        while index < values.len() {
            let boundary = values[index];
            let mut count = 1;
            while index + count < values.len()
                && values[index + count].to_bits() == boundary.to_bits()
            {
                count += 1;
            }
            if wanted >= count {
                wanted -= count;
                if wanted == 0 {
                    return Ok(boundary);
                }
            } else {
                let upper = boundary.log2();
                let lower = previous.map_or(upper - 1.0, f64::log2);
                return Ok((lower
                    + (upper - lower) * f64_from_usize(wanted)? / f64_from_usize(count)?)
                .exp2());
            }
            previous = Some(boundary);
            index += count;
        }
        Ok(*values.last().expect("nonempty quantile observations"))
    }
}

// Tempo's shared Kahan/Neumaier increment, including its infinity branch.
fn kahan_increment(sum: f64, increment: f64, mut compensation: f64) -> (f64, f64) {
    let result = sum + increment;
    if result.is_infinite() {
        compensation = 0.0;
    } else if sum.abs() >= increment.abs() {
        compensation += (sum - result) + increment;
    } else {
        compensation += (increment - result) + sum;
    }
    (result, compensation)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn incremental_average_stays_finite_when_the_sum_overflows() {
        let mut bucket = MetricBucket::default();
        bucket.record(Some(1e308), None);
        bucket.record(Some(1e308), None);
        assert!(bucket.sum.is_infinite());
        assert!(bucket.average().to_bits() == 1e308_f64.to_bits());
        assert!(bucket.min.unwrap().to_bits() == 1e308_f64.to_bits());
    }
}
