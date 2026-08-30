use super::*;

pub(crate) fn compactor_object_store<'a>(
    object_store: Option<&'a dyn ObjectStore>,
    configured_store: Option<&'a ConfiguredObjectStore>,
) -> Result<(&'a dyn ObjectStore, Option<&'a ObjectPath>), ServiceConfigError> {
    if let Some(store) = object_store {
        return Ok((store, None));
    }

    let configured_store = configured_store.ok_or(ServiceConfigError::MissingObjectStore)?;
    Ok((
        configured_store.store.as_ref(),
        Some(&configured_store.prefix),
    ))
}
