use super::{Labels, discover_detected_level_label};

pub(crate) fn loki_push_entry_labels(stream_labels: &Labels, line: &str) -> Labels {
    let mut labels = stream_labels.clone();
    discover_detected_level_label(&mut labels, line);
    labels
}
