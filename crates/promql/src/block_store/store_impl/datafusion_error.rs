use super::PromqlError;

pub(crate) fn datafusion_error(error: datafusion::error::DataFusionError) -> PromqlError {
    let message = error.to_string();
    drop(error);
    PromqlError::Store(message)
}
