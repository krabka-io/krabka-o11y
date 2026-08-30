use super::{BackendError, CatalogError};

/// Map a block-catalog enumeration failure to a backend transport error.
///
/// The endpoint then surfaces a 5xx instead of a silent return of only
/// live-tier results.
///
/// A search or tag query **partitions** the data across the live tier and
/// disjoint cold blocks. An empty block set from a catalog error looks the same
/// as "no cold blocks". To swallow the error with `unwrap_or_default` would
/// therefore drop the cold partitions and return a misleading `200`. This
/// matches the partitioning-shard-errors-must-surface contract that per-job
/// search errors already follow.
pub(crate) fn catalog_error(err: &CatalogError) -> BackendError {
    BackendError::Transport(err.to_string())
}
