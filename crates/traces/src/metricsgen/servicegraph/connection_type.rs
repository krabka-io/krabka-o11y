use krabka_domain_macros::EnumName;

/// Tempo service-graph connection classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, EnumName)]
#[enum_name(accessor = "as_label")]
pub enum ConnectionType {
    #[name(value = "unset")]
    Unset,
    #[name(value = "virtual_node")]
    VirtualNode,
    #[name(value = "messaging_system")]
    MessagingSystem,
    #[name(value = "database")]
    Database,
}
