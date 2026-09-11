use super::AuditPrincipal;

/// The principal `name`, authenticated with `mechanism`.
///
/// `mechanism` is one of [`MECHANISMS`](super::MECHANISMS).
#[must_use]
pub fn principal(name: impl Into<String>, mechanism: &'static str) -> AuditPrincipal {
    AuditPrincipal {
        name: name.into(),
        auth_method: mechanism.to_owned(),
    }
}
