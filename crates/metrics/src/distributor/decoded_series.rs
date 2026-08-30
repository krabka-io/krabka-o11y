use super::{DecodedSample, DecodedSeries};

pub(crate) fn decoded_series(labels: Vec<(String, String)>, sample: Option<DecodedSample>) -> DecodedSeries {
    DecodedSeries {
        labels: labels.into_iter().collect(),
        samples: sample.into_iter().collect(),
        histograms: Vec::new(),
        exemplars: Vec::new(),
        metadata: None,
    }
}
