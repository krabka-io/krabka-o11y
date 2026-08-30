use super::{Parser, IngestRateBucketCap};

pub(crate) fn parse_ingest_rate_bucket_cap(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|error| error.to_string())
        .and_then(IngestRateBucketCap::new)
        .map(IngestRateBucketCap::get)
}
