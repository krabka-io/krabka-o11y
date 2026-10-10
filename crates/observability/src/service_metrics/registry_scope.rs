use super::{Arc, Mutex, Registry, RoleKind, SharedRegistry};

/// Where [`register_for_role`] registers a role's instruments: a sub-registry
/// of the process registry, prefixed `<signal_prefix>_<role>`.
pub struct RoleRegistry {
    /// The process registry.
    pub shared: SharedRegistry,
    /// The signal's prefix, such as `krabka_metrics`.
    pub signal_prefix: &'static str,
    /// The role whose instruments are registered.
    pub role: RoleKind,
}

/// Creates a private registry with `prefix`, runs `register` against it, and
/// returns what `register` builds.
///
/// # Panics
/// Panics if the newly created private registry is locked during
/// construction, which cannot happen.
pub fn register_in_new_registry<T>(
    prefix: &'static str,
    register: impl FnOnce(&mut Registry, SharedRegistry) -> T,
) -> T {
    let shared = Arc::new(Mutex::new(Registry::with_prefix(prefix)));
    let mut registry = shared.try_lock().expect("new registry is unlocked");
    register(&mut registry, Arc::clone(&shared))
}

/// Runs `register` against a role's sub-registry of the process registry.
///
/// Each role has a separate prefix, so its gauges do not overwrite another
/// role's gauges.
pub async fn register_for_role<T>(
    scope: RoleRegistry,
    register: impl FnOnce(&mut Registry, SharedRegistry) -> T,
) -> T {
    let mut root = scope.shared.lock().await;
    let registry = root.sub_registry_with_prefix(format!(
        "{}_{}",
        scope.signal_prefix,
        scope.role.as_str().replace('-', "_")
    ));
    register(registry, Arc::clone(&scope.shared))
}
