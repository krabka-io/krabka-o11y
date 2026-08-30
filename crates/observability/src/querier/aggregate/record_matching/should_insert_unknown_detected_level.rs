use super::*;

pub(crate) fn should_insert_unknown_detected_level(labels: &Labels) -> bool {
    !labels.contains_key("detected_level")
        && !labels.contains_key("level")
        && !labels.contains_key("severity")
        && !labels.contains_key("severity_text")
}
