use super::*;

pub(crate) fn service_graph_labels((client, server, connection_type): LabelKey) -> Vec<(String, String)> {
    sorted_labels(vec![
        ("client".to_string(), client),
        ("server".to_string(), server),
        (
            "connection_type".to_string(),
            connection_type.as_label().to_string(),
        ),
    ])
}
