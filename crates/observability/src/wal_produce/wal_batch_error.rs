/// A WAL batch that did not append in full, and how far it got.
///
/// `appended` is the count of records the broker acked. `total` is the count
/// the request asked for. `appended` is always below `total`, and a value
/// above zero means the batch landed in part. The handler that maps this to a
/// status code must not report a partial batch as a success.
#[derive(Debug, thiserror::Error)]
#[error("wal append wrote {appended} of {total} records: {source}")]
pub struct WalBatchError<E>
where
    E: std::error::Error + 'static,
{
    appended: usize,
    total: usize,
    #[source]
    source: E,
}

impl<E> WalBatchError<E>
where
    E: std::error::Error + 'static,
{
    /// Builds the error from the counts and the failure that stopped the batch.
    #[must_use]
    pub const fn new(appended: usize, total: usize, source: E) -> Self {
        Self {
            appended,
            total,
            source,
        }
    }

    /// Returns the count of records the broker acked.
    #[must_use]
    pub const fn appended(&self) -> usize {
        self.appended
    }

    /// Returns the count of records the request asked to append.
    #[must_use]
    pub const fn total(&self) -> usize {
        self.total
    }

    /// Reports whether part of the batch is durable.
    ///
    /// A partial batch is the case a retry duplicates, because every upstream
    /// client here retries the whole request.
    #[must_use]
    pub const fn is_partial(&self) -> bool {
        self.appended > 0
    }

    /// Returns the failure that stopped the batch.
    #[must_use]
    pub const fn source(&self) -> &E {
        &self.source
    }

    /// Consumes the error and returns the failure that stopped the batch.
    #[must_use]
    pub fn into_source(self) -> E {
        self.source
    }
}
