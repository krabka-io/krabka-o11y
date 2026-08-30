use super::HashMap;

/// Evicts one arbitrary tenant from a per-tenant map to keep its size bounded.
pub(crate) fn evict_one_tenant<V>(map: &mut HashMap<String, V>) {
    if let Some(victim) = map.keys().next().cloned() {
        map.remove(&victim);
    }
}
