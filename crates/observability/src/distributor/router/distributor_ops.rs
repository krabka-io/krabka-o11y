use super::*;

pub(crate) const DISTRIBUTOR_OPS: RoleOps = RoleOps {
    target: "distributor",
    ring_component: "krabka-distributor",
    role_ring_path: Some("/distributor/ring"),
};
