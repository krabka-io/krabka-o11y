use super::ParquetError;

pub(crate) fn to_parquet_error(error: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}
