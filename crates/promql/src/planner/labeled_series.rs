use super::{Arc, Labels, SeriesFingerprint, TimedValue};

/// One matched series and its samples inside a scan window.
///
/// The operator leaves take their input in this shape rather than as a flat
/// list of labelled samples. A range query attaches the same label set to every
/// sample at every step, so carrying it per series instead of per sample is the
/// difference between one shared pointer and a deep copy of a
/// `BTreeMap<String, String>` for each of a window's samples. Sharing the label
/// set also lets a leaf write a label column run by run rather than looking the
/// name up once per row.
pub struct LabeledSeries {
    pub fp: SeriesFingerprint,
    pub labels: Arc<Labels>,
    /// The series' samples, ascending by timestamp.
    pub samples: Vec<TimedValue>,
}

#[cfg(test)]
impl LabeledSeries {
    /// A float series labelled only with `job` and holding no samples yet, for
    /// the range-leaf tests.
    pub(crate) fn with_job(job: &str) -> Self {
        let mut labels = Labels::new();
        labels.insert("job", job);
        Self {
            fp: labels.fingerprint(),
            labels: Arc::new(labels),
            samples: Vec::new(),
        }
    }

    /// Appends a float sample; callers append in timestamp order.
    pub(crate) fn at(mut self, ts_ms: i64, sample_value: f64) -> Self {
        self.samples.push(TimedValue {
            ts_ms,
            value: sample_value,
            start_timestamp_ms: None,
        });
        self
    }
}
