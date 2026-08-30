use super::{Result, RecordBatch, StreamReader, TraceqlError};

pub(crate) fn decode_span_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let reader =
        StreamReader::try_new(bytes, None).map_err(|err| TraceqlError::Plan(err.to_string()))?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|err| TraceqlError::Plan(err.to_string()))
}
