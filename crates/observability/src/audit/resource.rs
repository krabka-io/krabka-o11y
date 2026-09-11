use super::AuditResource;

/// The resource `name` of the type `resource_type`.
///
/// `resource_type` is one of [`RESOURCE_TYPES`](super::RESOURCE_TYPES).
#[must_use]
pub fn resource(resource_type: &'static str, name: impl Into<String>) -> AuditResource {
    AuditResource {
        resource_type: resource_type.to_owned(),
        name: name.into(),
    }
}
