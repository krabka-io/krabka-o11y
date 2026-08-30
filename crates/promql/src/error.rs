//! The crate's error type.

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn datafusion_error_maps_to_exec() {
        let dfe = datafusion::error::DataFusionError::Plan("boom".into());
        let pe: PromqlError = dfe.into();
        assert2::assert!(matches!(pe, PromqlError::Exec(_)));
    }

    #[test]
    fn display_includes_category() {
        let e = PromqlError::Unsupported("histogram_quantile".into());
        assert2::assert!(format!("{e}").contains("unsupported"));
    }
}

// === split-modules: generated submodules ===
mod promql_error;
mod result;

pub use promql_error::PromqlError;
pub use result::Result;
