use super::{Edge, LabelKey};

pub(crate) fn label_key_for_edge(edge: &Edge) -> LabelKey {
    LabelKey {
        client: edge.client_service.clone().unwrap_or_default(),
        server: edge.server_service.clone().unwrap_or_default(),
        connection_type: edge.connection_type,
        dimensions: edge.labels.clone(),
    }
}
