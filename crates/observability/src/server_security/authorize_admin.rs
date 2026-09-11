use super::{AdminDenied, Principal};

/// Checks that `principal` may call an admin operation, such as `POST /log_level`.
///
/// An [`Principal::Unauthenticated`] request always passes. An authenticated
/// principal passes only with `admin: true` in the credentials file.
///
/// # Errors
///
/// Returns [`AdminDenied`] for an authenticated principal without `admin`.
/// The denial is also reported to the principal's
/// [`SecurityEvents`](super::SecurityEvents).
pub fn authorize_admin(principal: &Principal) -> Result<(), AdminDenied> {
    match principal {
        Principal::Unauthenticated | Principal::Authenticated { admin: true, .. } => Ok(()),
        Principal::Authenticated {
            name,
            method,
            events,
            ..
        } => {
            events.events().admin_denied(name, *method);
            Err(AdminDenied {
                principal: name.clone(),
            })
        }
    }
}
