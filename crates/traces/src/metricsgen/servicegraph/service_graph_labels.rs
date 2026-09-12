use super::{LabelKey, sorted_labels};

pub(crate) fn service_graph_labels(
    (client, server, connection_type, mut dimensions): LabelKey,
) -> Vec<(String, String)> {
    dimensions.extend([
        ("client".to_string(), client),
        ("server".to_string(), server),
        (
            "connection_type".to_string(),
            connection_type.as_label().to_string(),
        ),
    ]);
    sorted_labels(dimensions)
}
