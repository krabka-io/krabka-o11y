use super::PromqlError;

pub(crate) fn cache_store_error(error: &object_store::Error) -> PromqlError {
    PromqlError::Store(format!("query frontend cache object-store error: {error}"))
}
