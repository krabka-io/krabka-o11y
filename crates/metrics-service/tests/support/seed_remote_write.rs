//! The shared differential seed dataset as one `remote_write` v1 body.

use krabka_metrics::wire::pb;
use prost::Message as _;

use crate::diff_corpus::seed_dataset;

pub fn remote_write_body() -> Vec<u8> {
    let req = pb::v1::WriteRequest {
        timeseries: seed_dataset()
            .into_iter()
            .map(|point| pb::v1::TimeSeries {
                labels: remote_write_labels(point.metric, point.labels),
                samples: point
                    .samples
                    .iter()
                    .map(|(timestamp, value)| pb::v1::Sample {
                        value: *value,
                        timestamp: *timestamp,
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

pub fn remote_write_labels(metric: &str, labels: &[(&str, &str)]) -> Vec<pb::v1::Label> {
    std::iter::once(pb::v1::Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    })
    .chain(labels.iter().map(|(name, value)| pb::v1::Label {
        name: (*name).to_string(),
        value: (*value).to_string(),
    }))
    .collect()
}
