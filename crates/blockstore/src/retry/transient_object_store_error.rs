use super::{Error, ObjectStoreError, is_transient_object_store_error};

/// The transient object-store failure behind `error`, if that is what it is.
///
/// The backend error is rarely the error a caller holds. A block write fails
/// as a [`ParquetError`](parquet::errors::ParquetError), because the Parquet
/// writer is what noticed the byte sink had gone; that wraps a
/// [`std::io::Error`], because that is the shape `AsyncWrite` has; and the
/// object-store error is inside *that*. So this walks the `source` chain and
/// classifies the first [`ObjectStoreError`] it meets.
///
/// Returning `None` covers two different answers on purpose -- "the backend
/// said 403" and "the backend was never involved, the block is malformed" --
/// because a retry loop owes both the same response: stop, and report.
#[must_use]
pub fn transient_object_store_error<'a>(
    error: &'a (dyn Error + 'static),
) -> Option<&'a ObjectStoreError> {
    let mut current = Some(error);
    while let Some(link) = current {
        if let Some(store_error) = link.downcast_ref::<ObjectStoreError>() {
            return is_transient_object_store_error(store_error).then_some(store_error);
        }
        current = next_link(link);
    }
    None
}

/// The next error in the chain.
///
/// This is `source`, with one correction. [`std::io::Error::source`] does not
/// return the error the `io::Error` was built from -- it returns *that
/// error's* source, skipping a link. An object-store failure that reached a
/// caller through an `AsyncWrite` is exactly the skipped link, so
/// [`get_ref`](std::io::Error::get_ref) is what has to be asked instead.
fn next_link<'a>(link: &'a (dyn Error + 'static)) -> Option<&'a (dyn Error + 'static)> {
    match link.downcast_ref::<std::io::Error>() {
        Some(io_error) => io_error
            .get_ref()
            .map(|inner| inner as &(dyn Error + 'static))
            .or_else(|| link.source()),
        None => link.source(),
    }
}
