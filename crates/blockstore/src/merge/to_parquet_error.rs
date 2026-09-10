use super::ParquetError;

/// Wraps an object-store error so the Parquet reader can carry it, and a
/// caller can get it back out.
///
/// `ParquetError::External` keeps the error whole rather than stringifying it,
/// which is what lets
/// [`BlockReadFailure`](crate::BlockReadFailure) tell a missing block from a
/// store that is down after the failure has been through the Parquet reader.
pub(crate) fn to_parquet_error(error: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}
