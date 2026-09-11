use super::RoleOps;

/// The ops identity a `--target all` process presents.
///
/// `Loki`'s single binary reports `target: all` and runs every module, and a
/// runbook pointed at this process should read the same. The ring page is the
/// distributor's, because that is the ring a `Loki` operator looks for on a
/// process that accepts pushes.
pub(crate) const ALL_OPS: RoleOps = RoleOps {
    target: "all",
    ring_component: "krabka-all",
    role_ring_path: Some("/distributor/ring"),
};
