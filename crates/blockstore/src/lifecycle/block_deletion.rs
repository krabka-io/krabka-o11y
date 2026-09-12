/// One block to delete, with the sidecar objects that belong to it.
///
/// A sidecar is an object a signal writes beside its block and names after it:
/// the profiles path writes a `{block}.symdb` symbol database, and the metrics
/// path writes a `{block}.index` manifest. Nothing but the block names a
/// sidecar, so a deletion that took the block alone would leave the sidecar on
/// object storage for as long as the bucket lives, and no later sweep would
/// find it. The block and its sidecars therefore travel together.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BlockDeletion {
    /// The block object itself.
    pub object_key: String,
    /// Objects written beside the block and named after it. Empty for a signal
    /// that writes none.
    pub sidecars: Vec<String>,
}
