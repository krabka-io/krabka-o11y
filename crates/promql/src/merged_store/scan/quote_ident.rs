use super::*;

/// Quotes a SQL identifier for safe interpolation into a `DataFusion` query.
pub(crate) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}
