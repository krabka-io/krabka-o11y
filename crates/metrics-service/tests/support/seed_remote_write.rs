//! The shared differential seed dataset as one `remote_write` v1 body.

use krabka_metrics::wire::pb;
use prost::Message as _;

use crate::diff_corpus::seed_dataset;

pub fn remote_write_body() -> Vec<u8> {
    let req = pb::v1::WriteRequest {
        timeseries: seed_dataset()
            .into_iter()
            .map(|point| pb::v1::TimeSeries {
                labels: remote_write_labels(
                    point.metric,
                    point.labels.iter().map(|label| FixtureLabel {
                        name: label.name,
                        value: label.value,
                    }),
                ),
                samples: point
                    .samples
                    .iter()
                    .map(|sample| pb::v1::Sample {
                        value: sample.value,
                        timestamp: sample.timestamp_ms,
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy remote_write")
}

/// One label of a fixture series, other than its metric name.
#[derive(Clone, Copy)]
pub struct FixtureLabel<'a> {
    pub name: &'a str,
    pub value: &'a str,
}

/// The `remote_write` labels of the series `metric{labels}`, name first.
pub fn remote_write_labels<'a>(
    metric: &str,
    labels: impl IntoIterator<Item = FixtureLabel<'a>>,
) -> Vec<pb::v1::Label> {
    std::iter::once(pb::v1::Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    })
    .chain(labels.into_iter().map(|label| pb::v1::Label {
        name: label.name.to_string(),
        value: label.value.to_string(),
    }))
    .collect()
}
