use super::{SpanRecord, ConnectionType, has_attr};

pub(crate) fn classify(span: &SpanRecord) -> ConnectionType {
    if has_attr(span, "db.system") {
        ConnectionType::Database
    } else if has_attr(span, "messaging.system") {
        ConnectionType::MessagingSystem
    } else if has_attr(span, "peer.service") {
        ConnectionType::VirtualNode
    } else {
        ConnectionType::Unset
    }
}
