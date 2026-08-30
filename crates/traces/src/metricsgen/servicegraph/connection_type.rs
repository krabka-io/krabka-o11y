
/// Tempo service-graph connection classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConnectionType {
    Unset,
    VirtualNode,
    MessagingSystem,
    Database,
}

impl ConnectionType {
    #[must_use]
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Unset => "unset",
            Self::VirtualNode => "virtual_node",
            Self::MessagingSystem => "messaging_system",
            Self::Database => "database",
        }
    }
}
