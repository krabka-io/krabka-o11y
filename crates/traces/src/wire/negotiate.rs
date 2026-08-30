use super::{WireError, WireFormat};

/// Pick the decoder from the request path.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn negotiate(path: &str, content_type: Option<&str>) -> Result<WireFormat, WireError> {
    match path {
        "/v1/traces" | "/api/push" => Ok(WireFormat::Otlp),
        "/api/v2/spans" => Ok(WireFormat::Zipkin),
        "/api/traces" => Ok(WireFormat::Jaeger),
        other => Err(WireError::UnsupportedContentType(format!(
            "{other} (content-type {})",
            content_type.unwrap_or("none")
        ))),
    }
}
