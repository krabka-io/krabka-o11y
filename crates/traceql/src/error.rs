//! `TraceQL` error categories.

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn datafusion_error_maps_to_exec() {
        let dfe = datafusion::error::DataFusionError::Plan("boom".into());
        let te: TraceqlError = dfe.into();
        assert!(matches!(te, TraceqlError::Exec(_)));
    }

    #[test]
    fn display_includes_category() {
        let e = TraceqlError::Unsupported("negated structural op".into());
        assert!(format!("{e}").contains("unsupported"));
    }
}

mod result;
mod traceql_error;

pub use result::Result;
pub use traceql_error::TraceqlError;
