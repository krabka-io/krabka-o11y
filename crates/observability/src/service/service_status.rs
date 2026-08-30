use super::Role;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceStatus {
    pub role: Role,
}
