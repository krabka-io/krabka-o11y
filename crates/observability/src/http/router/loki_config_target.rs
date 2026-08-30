/// The `target` that `/config` reports, for every role.
///
/// `Loki` reports the components its process runs. Krabka serves the full `Loki`
/// surface from each role, so its ops endpoints answer as single-binary `Loki`
/// does: [`status_services`] lists every component whichever role serves it,
/// and `/config` reports the target that goes with that list.
/// `real_loki_and_krabka_return_same_stable_config_status_lines` compares this
/// against a real `Loki` container, which reports `all`.
///
/// The per-role name stays in [`RoleOps::target`] for `/metrics`, where `Loki`
/// does report the running component.
pub(crate) const LOKI_CONFIG_TARGET: &str = "all";
