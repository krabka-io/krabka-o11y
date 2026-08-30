use super::{BucketSpan, Line, Result, parse_error};

pub(crate) fn histogram_span(offset: i32, len: usize, line: Line<'_>) -> Result<Option<BucketSpan>> {
    if len == 0 {
        return Ok(None);
    }
    Ok(Some(BucketSpan {
        offset,
        length: u32::try_from(len)
            .map_err(|error| parse_error(line, format!("too many histogram buckets: {error}")))?,
    }))
}
