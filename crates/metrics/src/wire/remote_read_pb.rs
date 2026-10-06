//! Prometheus gogo protobuf permits arbitrary bytes in label string fields.
//! These messages retain the pinned v3.14 wire tags while using byte values.
//! Native UTF-8 messages and these messages encode identically.

pub mod v1 {
    pub use super::super::pb::v1::{
        BucketSpan, Chunk, Histogram, ReadHints, ResponseType, Sample, histogram, label_matcher,
    };

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Label {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(bytes = "vec", tag = "2")]
        pub value: Vec<u8>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct LabelMatcher {
        #[prost(enumeration = "label_matcher::Type", tag = "1")]
        pub r#type: i32,
        #[prost(string, tag = "2")]
        pub name: String,
        #[prost(bytes = "vec", tag = "3")]
        pub value: Vec<u8>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Query {
        #[prost(int64, tag = "1")]
        pub start_timestamp_ms: i64,
        #[prost(int64, tag = "2")]
        pub end_timestamp_ms: i64,
        #[prost(message, repeated, tag = "3")]
        pub matchers: Vec<LabelMatcher>,
        #[prost(message, optional, tag = "4")]
        pub hints: Option<ReadHints>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ReadRequest {
        #[prost(message, repeated, tag = "1")]
        pub queries: Vec<Query>,
        #[prost(enumeration = "ResponseType", repeated, tag = "2")]
        pub accepted_response_types: Vec<i32>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Exemplar {
        #[prost(message, repeated, tag = "1")]
        pub labels: Vec<Label>,
        #[prost(double, tag = "2")]
        pub value: f64,
        #[prost(int64, tag = "3")]
        pub timestamp: i64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct TimeSeries {
        #[prost(message, repeated, tag = "1")]
        pub labels: Vec<Label>,
        #[prost(message, repeated, tag = "2")]
        pub samples: Vec<Sample>,
        #[prost(message, repeated, tag = "3")]
        pub exemplars: Vec<Exemplar>,
        #[prost(message, repeated, tag = "4")]
        pub histograms: Vec<Histogram>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct QueryResult {
        #[prost(message, repeated, tag = "1")]
        pub timeseries: Vec<TimeSeries>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ReadResponse {
        #[prost(message, repeated, tag = "1")]
        pub results: Vec<QueryResult>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ChunkedSeries {
        #[prost(message, repeated, tag = "1")]
        pub labels: Vec<Label>,
        #[prost(message, repeated, tag = "2")]
        pub chunks: Vec<Chunk>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct ChunkedReadResponse {
        #[prost(message, repeated, tag = "1")]
        pub chunked_series: Vec<ChunkedSeries>,
        #[prost(int64, tag = "2")]
        pub query_index: i64,
    }

    impl From<super::super::pb::v1::ReadResponse> for ReadResponse {
        fn from(response: super::super::pb::v1::ReadResponse) -> Self {
            Self {
                results: response
                    .results
                    .into_iter()
                    .map(|result| QueryResult {
                        timeseries: result
                            .timeseries
                            .into_iter()
                            .map(|series| TimeSeries {
                                labels: series
                                    .labels
                                    .into_iter()
                                    .map(|label| Label {
                                        name: label.name,
                                        value: label.value.into_bytes(),
                                    })
                                    .collect(),
                                samples: series.samples,
                                exemplars: series
                                    .exemplars
                                    .into_iter()
                                    .map(|exemplar| Exemplar {
                                        labels: exemplar
                                            .labels
                                            .into_iter()
                                            .map(|label| Label {
                                                name: label.name,
                                                value: label.value.into_bytes(),
                                            })
                                            .collect(),
                                        value: exemplar.value,
                                        timestamp: exemplar.timestamp,
                                    })
                                    .collect(),
                                histograms: series.histograms,
                            })
                            .collect(),
                    })
                    .collect(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::v1;

    #[test]
    fn gogo_label_wire_preserves_invalid_bytes_and_native_messages() {
        // Pinned prompb/types.pb.go MarshalToSizedBuffer copies Value directly
        // at tag 2: length-delimited bytes, without UTF-8 validation.
        let golden = [0x0a, 3, b'r', b'a', b'w', 0x12, 1, 0xff];
        let label = v1::Label::decode(golden.as_slice()).unwrap();
        assert2::assert!(label.name == "raw" && label.value == [0xff]);
        assert2::assert!(label.encode_to_vec() == golden);
        assert2::assert!(crate::wire::pb::v1::Label::decode(golden.as_slice()).is_err());
        let native = crate::wire::pb::v1::ReadResponse {
            results: vec![crate::wire::pb::v1::QueryResult {
                timeseries: vec![crate::wire::pb::v1::TimeSeries {
                    labels: vec![crate::wire::pb::v1::Label {
                        name: "raw".into(),
                        value: "native".into(),
                    }],
                    samples: vec![v1::Sample {
                        value: 2.0,
                        timestamp: 10_000,
                    }],
                    exemplars: vec![crate::wire::pb::v1::Exemplar {
                        labels: vec![crate::wire::pb::v1::Label {
                            name: "trace_id".into(),
                            value: "abc".into(),
                        }],
                        value: 1.0,
                        timestamp: 10_000,
                    }],
                    histograms: Vec::new(),
                }],
            }],
        };
        assert2::assert!(
            v1::ReadResponse::from(native.clone()).encode_to_vec() == native.encode_to_vec()
        );
    }
}
