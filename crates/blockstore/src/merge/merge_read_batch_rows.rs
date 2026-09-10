/// Rows per batch a merge decodes from each of its inputs.
///
/// A k-way merge holds one decoded batch of every input at once, so this is
/// the term that scales with the number of blocks being merged rather than
/// with their size, and it is deliberately smaller than the batch the merge
/// emits. Span rows are wide -- nested events, links and attribute lists --
/// and a thousand of them per input is a few megabytes across a dozen inputs
/// where eight thousand would be tens. The emitted batch is built by
/// concatenating slices of these, so a smaller read batch costs a few more
/// slices per output batch and nothing else.
pub const MERGE_READ_BATCH_ROWS: usize = 1_024;
