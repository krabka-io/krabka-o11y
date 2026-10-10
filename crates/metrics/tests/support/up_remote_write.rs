//! The smallest `remote_write` push a distributor accepts.

use krabka_metrics::wire::pb;
use prost::Message as _;

/// Minimal `remote_write` v1 body. It holds a single `up` series with one
/// sample, snappy compressed, because the distributor requires
/// `Content-Encoding: snappy`.
pub fn remote_write_v1_body() -> Vec<u8> {
    let req = pb::v1::WriteRequest {
        timeseries: vec![pb::v1::TimeSeries {
            labels: vec![pb::v1::Label {
                name: "__name__".into(),
                value: "up".into(),
            }],
            samples: vec![pb::v1::Sample {
                value: 1.0,
                timestamp: 100,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy compress")
}
