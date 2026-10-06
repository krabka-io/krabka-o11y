use super::{Labels, insert_json_parser_error, sanitize_json_field_name};

pub(crate) fn unpack_json_line(
    line: &mut String,
    fields: &mut Labels,
    resolve_name: impl Fn(&str) -> Option<String>,
) {
    if line.is_empty() {
        return;
    }
    if !line.starts_with('{') {
        insert_json_parser_error(fields);
        fields.insert(
            "__error_details__".into(),
            "expecting json object(6), but it is not".into(),
        );
        return;
    }
    let Ok(entries) = crate::parse_json_object_entries(line) else {
        insert_json_parser_error(fields);
        return;
    };

    // Loki buffers map[string]string labels and commits them only after a
    // string _entry is found. The last duplicate _entry and label win.
    let mut replacement = None;
    let mut parsed = Labels::new();
    for (name, value) in entries {
        if !value.get().starts_with('"') {
            continue;
        }
        let Ok(value) = serde_json::from_str::<String>(value.get()) else {
            insert_json_parser_error(fields);
            return;
        };
        if name == "_entry" {
            replacement = Some(value);
        } else if let Some(name) = resolve_name(&name) {
            parsed.insert(
                sanitize_json_field_name(&name),
                value.replace('\u{fffd}', " "),
            );
        }
    }
    if let Some(entry) = replacement {
        fields.extend(parsed);
        *line = entry;
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use crate::{Labels, parse_query};

    #[test]
    fn unpack_commits_only_string_labels_with_an_entry_and_last_duplicates_win() {
        let labels = Labels::from([
            ("app".into(), "api".into()),
            ("token".into(), "base".into()),
        ]);
        let query = parse_query(r#"{app="api"} | unpack"#).unwrap();
        let line = r#"{"token":"first","token":"last","n":1e3,"fixed":1.00,"obj":{"x":"ignored"},"array":["ignored"],"_entry":"first","_entry":42,"_entry":"final\nline"}trailing"#;
        let result = query
            .evaluate_with_fields(&labels, line, &Labels::new())
            .unwrap();
        check!(
            (result.line, result.fields)
                == (
                    "final\nline".into(),
                    Labels::from([
                        ("app".into(), "api".into()),
                        ("token".into(), "base".into()),
                        ("token_extracted".into(), "last".into()),
                    ])
                )
        );
        for line in [
            r#"{"token":"value","nested":{"x":"ignored"}}"#,
            r#"{"token":"value","_entry":null}"#,
            "",
        ] {
            let result = query
                .evaluate_with_fields(&labels, line, &Labels::new())
                .unwrap();
            check!((result.line.as_str(), result.fields) == (line, labels.clone()));
        }
    }

    #[test]
    fn unpack_checks_collisions_before_sanitizing_packed_keys() {
        let labels = Labels::from([
            ("app".into(), "api".into()),
            ("foo_bar".into(), "base".into()),
            ("token".into(), "base_token".into()),
            ("token_extracted".into(), "older_base".into()),
        ]);
        let metadata = Labels::from([("meta_key".into(), "metadata".into())]);
        let query = parse_query(r#"{app="api"} | unpack"#).unwrap();
        let result = query
            .evaluate_with_fields(
                &labels,
                r#"{"foo.bar":"packed","token":"last","meta.key":"packed_meta","_entry":"body"}"#,
                &metadata,
            )
            .unwrap();
        check!(
            (result.line, result.fields)
                == (
                    "body".into(),
                    Labels::from([
                        ("app".into(), "api".into()),
                        ("foo_bar".into(), "packed".into()),
                        ("token".into(), "base_token".into()),
                        ("token_extracted".into(), "last".into()),
                        ("meta_key".into(), "packed_meta".into()),
                    ])
                )
        );
    }

    #[test]
    fn unpack_sanitizes_in_input_order_and_does_not_publish_partial_failed_labels() {
        let labels = Labels::from([("app".into(), "api".into())]);
        let query = parse_query(r#"{app="api"} | unpack"#).unwrap();
        let result = query
            .evaluate_with_fields(
                &labels,
                r#"{"a-b":"early","a.b":"late","token":"a\ufffdb","_entry":"x\ufffdy"}"#,
                &Labels::new(),
            )
            .unwrap();
        check!(
            (result.line, result.fields)
                == (
                    "x\u{fffd}y".into(),
                    Labels::from([
                        ("app".into(), "api".into()),
                        ("a_b".into(), "late".into()),
                        ("token".into(), "a b".into()),
                    ])
                )
        );
        let line = r#"{"token":"must_not_escape","_entry":"body""#;
        let result = query
            .evaluate_with_fields(&labels, line, &Labels::new())
            .unwrap();
        check!(result.line == line);
        check!(!result.fields.contains_key("token"));
        check!(result.fields.get("__error__").map(String::as_str) == Some("JSONParserErr"));
        let rejected = parse_query(r#"{app="api"} | unpack | __error__="""#).unwrap();
        check!(
            rejected
                .evaluate_with_fields(&labels, line, &Labels::new())
                .is_none()
        );
    }
}
