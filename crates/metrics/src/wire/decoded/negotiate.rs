use super::{WireError, WireFormat, proto_param_value};

/// Dispatches on the `proto=` parameter of the `Content-Type` header. A bare
/// `application/x-protobuf` stays the v1 default.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn negotiate(content_type: Option<&str>) -> Result<WireFormat, WireError> {
    let Some(content_type) = content_type else {
        return Ok(WireFormat::RemoteWriteV1);
    };
    let mut parts = content_type.split(';');
    let base = parts.next().unwrap_or_default().trim();
    if !base.eq_ignore_ascii_case("application/x-protobuf") {
        return Err(WireError::UnsupportedContentType(base.to_string()));
    }

    let proto = parts.find_map(proto_param_value);
    match proto.as_deref() {
        None | Some("prometheus.WriteRequest") => Ok(WireFormat::RemoteWriteV1),
        Some("io.prometheus.write.v2.Request") => Ok(WireFormat::RemoteWriteV2),
        Some(other) => Err(WireError::UnsupportedContentType(format!("proto={other}"))),
    }
}
