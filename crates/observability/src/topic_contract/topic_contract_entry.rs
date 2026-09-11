use super::TopicKind;

/// One topic in the contract: its name, what it is for, and which component
/// depends on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicContract {
    /// The topic name. Nothing derives it; the broker sees this string.
    pub name: &'static str,

    /// Whether the topic is a WAL or compacted state. This decides the
    /// configuration the contract requires.
    pub kind: TopicKind,

    /// What breaks when the topic is wrong. The provisioning errors quote it,
    /// so an operator reads the consequence rather than only the key.
    pub purpose: &'static str,
}
