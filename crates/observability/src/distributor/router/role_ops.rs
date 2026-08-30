use super::*;

#[derive(Clone, Copy)]
pub(crate) struct RoleOps {
    pub(crate) target: &'static str,
    pub(crate) ring_component: &'static str,
    pub(crate) role_ring_path: Option<&'static str>,
}
