//! A Prometheus `remote_write` v1 request built from fuzzer input.
//!
//! `decode_v1` takes a snappy-framed protobuf body, so a raw-bytes target
//! spends its budget on snappy and on prost's varint framing -- both already
//! hardened elsewhere and neither written here. The model below skips past
//! both: the fuzzer picks the field values, this code encodes them with prost
//! and compresses them with snap, and what the decoder then sees is a body a
//! well-behaved client could have sent, carrying values it never would.
//!
//! The native-histogram half is why this matters. `v1_histogram_to_native`
//! reads bucket spans, delta runs and count runs whose lengths have to agree
//! with each other, and reaching that code through random bytes needs a
//! protobuf a fuzzer will not stumble on.

use arbitrary::Arbitrary;
use krabka_metrics::wire::pb;

#[derive(Arbitrary, Debug)]
pub struct WriteRequestModel {
    pub timeseries: Vec<TimeSeriesModel>,
    pub metadata: Vec<MetadataModel>,
}

#[derive(Arbitrary, Debug)]
pub struct TimeSeriesModel {
    pub labels: Vec<LabelModel>,
    pub samples: Vec<SampleModel>,
    pub exemplars: Vec<ExemplarModel>,
    pub histograms: Vec<HistogramModel>,
}

#[derive(Arbitrary, Debug)]
pub struct LabelModel {
    pub name: String,
    pub value: String,
}

#[derive(Arbitrary, Debug)]
pub struct SampleModel {
    pub value: f64,
    pub timestamp: i64,
}

#[derive(Arbitrary, Debug)]
pub struct ExemplarModel {
    pub labels: Vec<LabelModel>,
    pub value: f64,
    pub timestamp: i64,
}

#[derive(Arbitrary, Debug)]
pub struct MetadataModel {
    pub metric_type: i32,
    pub metric_family_name: String,
    pub help: String,
    pub unit: String,
}

/// A native histogram. The span lengths, the delta runs and the count runs are
/// chosen independently, so most draws disagree with one another -- which is
/// the case `validate_spans_and_counts` exists for.
#[derive(Arbitrary, Debug)]
pub struct HistogramModel {
    pub count: CountModel,
    pub sum: f64,
    pub schema: i32,
    pub zero_threshold: f64,
    pub zero_count: CountModel,
    pub negative_spans: Vec<BucketSpanModel>,
    pub negative_deltas: Vec<i64>,
    pub negative_counts: Vec<f64>,
    pub positive_spans: Vec<BucketSpanModel>,
    pub positive_deltas: Vec<i64>,
    pub positive_counts: Vec<f64>,
    pub reset_hint: i32,
    pub timestamp: i64,
    pub custom_values: Vec<f64>,
}

#[derive(Arbitrary, Debug)]
pub enum CountModel {
    Absent,
    Int(u64),
    Float(f64),
}

#[derive(Arbitrary, Debug)]
pub struct BucketSpanModel {
    pub offset: i32,
    pub length: u32,
}

/// Encode a model as the snappy-framed protobuf body `decode_v1` takes.
#[must_use]
pub fn encode_v1(model: &WriteRequestModel) -> Vec<u8> {
    use prost::Message as _;

    let request = pb::v1::WriteRequest {
        timeseries: model.timeseries.iter().map(time_series).collect(),
        metadata: model.metadata.iter().map(metadata).collect(),
    };
    snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .unwrap_or_default()
}

fn time_series(model: &TimeSeriesModel) -> pb::v1::TimeSeries {
    pb::v1::TimeSeries {
        labels: model.labels.iter().map(label).collect(),
        samples: model
            .samples
            .iter()
            .map(|sample| pb::v1::Sample {
                value: sample.value,
                timestamp: sample.timestamp,
            })
            .collect(),
        exemplars: model
            .exemplars
            .iter()
            .map(|exemplar| pb::v1::Exemplar {
                labels: exemplar.labels.iter().map(label).collect(),
                value: exemplar.value,
                timestamp: exemplar.timestamp,
            })
            .collect(),
        histograms: model.histograms.iter().map(histogram).collect(),
    }
}

fn label(model: &LabelModel) -> pb::v1::Label {
    pb::v1::Label {
        name: model.name.clone(),
        value: model.value.clone(),
    }
}

fn metadata(model: &MetadataModel) -> pb::v1::MetricMetadata {
    pb::v1::MetricMetadata {
        r#type: model.metric_type,
        metric_family_name: model.metric_family_name.clone(),
        help: model.help.clone(),
        unit: model.unit.clone(),
    }
}

fn histogram(model: &HistogramModel) -> pb::v1::Histogram {
    pb::v1::Histogram {
        count: match model.count {
            CountModel::Absent => None,
            CountModel::Int(value) => Some(pb::v1::histogram::Count::CountInt(value)),
            CountModel::Float(value) => Some(pb::v1::histogram::Count::CountFloat(value)),
        },
        sum: model.sum,
        schema: model.schema,
        zero_threshold: model.zero_threshold,
        zero_count: match model.zero_count {
            CountModel::Absent => None,
            CountModel::Int(value) => Some(pb::v1::histogram::ZeroCount::ZeroCountInt(value)),
            CountModel::Float(value) => Some(pb::v1::histogram::ZeroCount::ZeroCountFloat(value)),
        },
        negative_spans: model.negative_spans.iter().map(bucket_span).collect(),
        negative_deltas: model.negative_deltas.clone(),
        negative_counts: model.negative_counts.clone(),
        positive_spans: model.positive_spans.iter().map(bucket_span).collect(),
        positive_deltas: model.positive_deltas.clone(),
        positive_counts: model.positive_counts.clone(),
        reset_hint: model.reset_hint,
        timestamp: model.timestamp,
        custom_values: model.custom_values.clone(),
    }
}

fn bucket_span(model: &BucketSpanModel) -> pb::v1::BucketSpan {
    pb::v1::BucketSpan {
        offset: model.offset,
        length: model.length,
    }
}
