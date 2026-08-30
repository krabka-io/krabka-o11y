use super::{BucketSpan, WireError, span_bucket_total};

pub(crate) fn check_side(side: &str, spans: &[BucketSpan], counts: usize) -> Result<(), WireError> {
    let expected = span_bucket_total(spans);
    if expected != counts {
        return Err(WireError::Invalid(format!(
            "{side} spans declare {expected} buckets but {counts} counts were decoded"
        )));
    }
    Ok(())
}
