use super::{
    ConditionalUpdateRequirement, ObjectStore, ObjectStoreAccess, ObjectStoreSemanticsError, Path,
    verify_object_store_read_access, verify_object_store_semantics,
};

/// Runs the startup probe that fits `access`.
///
/// [`ObjectStoreAccess::ReadWrite`] runs [`verify_object_store_semantics`]
/// with `requirement`. [`ObjectStoreAccess::ReadOnly`] runs
/// [`verify_object_store_read_access`], which writes nothing, and ignores
/// `requirement`.
///
/// # Errors
/// Returns the error of the probe that ran.
pub async fn verify_object_store_access(
    store: &dyn ObjectStore,
    prefix: &Path,
    access: ObjectStoreAccess,
    requirement: ConditionalUpdateRequirement,
) -> Result<(), ObjectStoreSemanticsError> {
    match access {
        ObjectStoreAccess::ReadWrite => verify_object_store_semantics(store, prefix, requirement)
            .await
            .map(drop),
        ObjectStoreAccess::ReadOnly => verify_object_store_read_access(store, prefix).await,
    }
}
