use super::RoleOps;

/// The ops identity the block builder presents on the `Loki` compat surface.
///
/// The role is `block-builder`; the strings here say `compactor`, and that is
/// deliberate. `Loki` has no block builder. The stage that writes durable
/// storage there is its compactor, and a `Loki` runbook, dashboard or
/// datasource reads `loki_boltdb_shipper_compactor_running` and the
/// `/compactor/ring` path to find it. Renaming these would rename a wire
/// contract to match an internal one, which is the wrong way round: the role
/// vocabulary is Krabka's, and these three strings are `Loki`'s.
pub(crate) const BLOCK_BUILDER_OPS: RoleOps = RoleOps {
    target: "compactor",
    ring_component: "krabka-compactor",
    role_ring_path: Some("/compactor/ring"),
};
