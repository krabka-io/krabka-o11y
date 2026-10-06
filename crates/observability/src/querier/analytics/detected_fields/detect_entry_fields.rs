use super::{
    BTreeMap, DetectedFieldStats, Labels, detect_json_fields, detect_logfmt_fields,
    detect_structured_metadata_fields,
};

/// Records every field one log entry shows: its structured
/// metadata, and what the JSON and logfmt parsers find in its line.
pub(crate) fn detect_entry_fields(
    fields: &mut BTreeMap<String, DetectedFieldStats>,
    line: &str,
    structured_metadata: &Labels,
) {
    detect_structured_metadata_fields(fields, structured_metadata);
    detect_json_fields(fields, line);
    detect_logfmt_fields(fields, line);
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{BTreeMap, Labels, detect_entry_fields};

    #[test]
    fn discovery_reports_existing_metadata_without_synthesizing_levels() {
        let mut fields = BTreeMap::new();
        detect_entry_fields(&mut fields, "plain error=none", &Labels::new());
        assert!(!fields.contains_key("detected_level"));
        let metadata = Labels::from([("detected_level".into(), "debug".into())]);
        detect_entry_fields(&mut fields, "level=debug error=none", &metadata);
        assert!(
            fields["detected_level"]
                .values
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                == ["debug"]
        );
        assert!(fields["detected_level"].parsers.is_empty());
    }
}
