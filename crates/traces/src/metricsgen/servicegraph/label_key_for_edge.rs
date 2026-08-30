use super::{Edge, LabelKey};

pub(crate) fn label_key_for_edge(edge: &Edge) -> LabelKey {
    (
        edge.client_service.clone().unwrap_or_default(),
        edge.server_service.clone().unwrap_or_default(),
        edge.connection_type,
    )
}
