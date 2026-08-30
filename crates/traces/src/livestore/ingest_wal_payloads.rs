use super::{LiveStore, SpanRecord, TracesError};

/// Decode WAL payloads and ingest them into the live store.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn ingest_wal_payloads<'a, I>(store: &mut LiveStore, payloads: I) -> Result<usize, TracesError>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    let mut count = 0;
    for payload in payloads {
        store.ingest(SpanRecord::decode(payload)?);
        count += 1;
    }
    Ok(count)
}
