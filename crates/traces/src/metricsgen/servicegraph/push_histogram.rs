use super::*;

pub(crate) fn push_histogram(
    out: &mut Vec<Series>,
    name: &str,
    labels: &[(String, String)],
    histogram: HistogramSnapshot<'_>,
    timestamp_ms: i64,
) {
    out.push(Series {
        name: name.to_string(),
        labels: labels.to_vec(),
        sample: SeriesSample::ClassicHistogram {
            buckets: cumulative_buckets_seconds(histogram.bucket_edges_ns, histogram.bucket_counts),
            sum: histogram.sum,
            count: histogram.count,
        },
        exemplars: Vec::new(),
        timestamp_ms,
    });
}
