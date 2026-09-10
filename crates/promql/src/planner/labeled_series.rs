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
