use super::{BlockSkipReason, PromqlError, Result, ScanReport};

/// The warnings for the blocks a scan left out, or an error when a skip is not
/// an absent object.
///
/// Only an absent object may be skipped. Compaction and retention delete
/// blocks while a reader still holds an index snapshot that names them, so a
/// metrics query meets a missing key in ordinary operation and a failed query
/// would be the wrong answer. Every other reason keeps its error. A block
/// whose bytes are there but do not decode says that the data is damaged, not
/// that it is gone, and a query that answered around it would hide the damage
/// behind a plausible result.
///
/// The scan has already registered the blocks it could read by the time this
/// runs, so a rejected report abandons that work. That is the intent: the
/// caller returns the error instead of the partial answer.
///
/// # The warning text names the block and nothing else
///
/// [`SkippedBlock`](krabka_blockstore::SkippedBlock) renders the backend error
/// into its `Display`, and this text leaves that out. The warning reaches an
/// API client in the `warnings` array of a Prometheus response, where the text
/// is part of the surface: an `object_store` message names the backend and the
/// bucket, reads differently on S3 than on a local directory, and says nothing
/// a caller of the query API can act on. A reader that wants the backend error
/// has it, because `probe_blocks` logs one `warn!` per skipped block with the
/// detail attached.
///
/// # Errors
/// Returns [`PromqlError::Store`] when a skipped block is present but
/// unreadable. That error does carry the backend detail, because an operator
/// reads it and a damaged block is theirs to act on.
pub(crate) fn missing_block_warnings(report: &ScanReport) -> Result<Vec<String>> {
    let mut warnings = Vec::with_capacity(report.skipped.len());
    for block in &report.skipped {
        if block.reason != BlockSkipReason::Missing {
            return Err(PromqlError::Store(block.to_string()));
        }
        warnings.push(format!(
            "block \"{}\" is missing from object storage and is not in this result",
            block.object_key
        ));
    }
    Ok(warnings)
}
