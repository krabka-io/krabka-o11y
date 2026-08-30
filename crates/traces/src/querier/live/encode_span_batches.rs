use super::*;

///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn encode_span_batches(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let Some(first) = batches.first() else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut out, &first.schema())
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        }
        writer
            .finish()
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
    }
    Ok(out)
}
