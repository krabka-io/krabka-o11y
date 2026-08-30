use super::*;

#[derive(Clone, Default)]
pub(crate) struct MetricBucket {
    pub(crate) count: u64,
    pub(crate) sum: f64,
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
        self.sum += value;
        self.min = Some(self.min.map_or(value, |min| min.min(value)));
        self.max = Some(self.max.map_or(value, |max| max.max(value)));
        self.values.push(value);
    }

    pub(crate) fn average(&self) -> Result<f64> {
        if self.count == 0 {
            Ok(0.0)
        } else {
            Ok(self.sum / f64_from_u64(self.count)?)
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
        let rank = quantile * f64_from_usize(values.len() - 1)?;
        let lower = usize_from_integer_f64(rank.floor())?;
        let upper = usize_from_integer_f64(rank.ceil())?;
        if lower == upper {
            Ok(values[lower])
        } else {
            Ok(values[lower] + (values[upper] - values[lower]) * (rank - f64_from_usize(lower)?))
        }
    }
}
