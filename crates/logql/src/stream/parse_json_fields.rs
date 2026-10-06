use super::{Labels, insert_json_parser_error, insert_raw_parsed_field, sanitize_json_field_name};
use crate::parse_json_object_entries;

pub(crate) fn parse_json_fields(line: &str, fields: &mut Labels) {
    let Ok(object) = parse_json_object_entries(line) else {
        insert_json_parser_error(fields);
        return;
    };
    flatten_entries(object, fields);
}

fn flatten_entries(entries: Vec<(String, &serde_json::value::RawValue)>, fields: &mut Labels) {
    // RawValue accepts deeper objects than serde's DOM recursion limit. Keep
    // ObjectEach's depth-first order on the heap, with one rollback prefix.
    let mut pending = vec![(0, entries.into_iter())];
    let mut name = String::new();
    while let Some((prefix_len, entries)) = pending.last_mut() {
        name.truncate(*prefix_len);
        let Some((key, value)) = entries.next() else {
            pending.pop();
            continue;
        };
        name.push_str(&sanitize_json_field_name(&key));
        let value = value.get();
        match value.as_bytes().first() {
            Some(b'{') => {
                if let Ok(entries) = parse_json_object_entries(value) {
                    name.push('_');
                    pending.push((name.len(), entries.into_iter()));
                }
            }
            Some(b'[' | b'n') => {}
            Some(b'"') => {
                if let Ok(value) = serde_json::from_str::<String>(value) {
                    insert_raw_parsed_field(fields, &name, value.replace('\u{fffd}', " "));
                }
            }
            _ => insert_raw_parsed_field(fields, &name, value.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{Labels, parse_json_fields};

    fn nested_line(depth: usize) -> String {
        format!(
            "{}{{\"level\":\"error\",\"n\":1e3}}{}",
            "{\"x\":".repeat(depth),
            "}".repeat(depth)
        )
    }

    #[test]
    fn deep_objects_match_pinned_loki_and_preserve_first_physical_value() {
        // Loki 3.7.7 JSONParser(false), grafana/jsonparser 023329977675:
        // depths 300/4096/10000 produce these values without JSONParserErr.
        let mut fields = Labels::new();
        parse_json_fields(&nested_line(300), &mut fields);
        assert!(
            fields
                == Labels::from([
                    (format!("{}level", "x_".repeat(300)), "error".to_string()),
                    (format!("{}n", "x_".repeat(300)), "1e3".to_string()),
                ])
        );

        let mut fields = Labels::new();
        parse_json_fields(
            r#"{"a":{"b":"first","b":"duplicate"},"a_b":"later","a":{"c":1.00},"tail":true}"#,
            &mut fields,
        );
        assert!(
            fields
                == Labels::from([
                    ("a_b".to_string(), "first".to_string()),
                    ("a_c".to_string(), "1.00".to_string()),
                    ("tail".to_string(), "true".to_string()),
                ])
        );
    }

    #[test]
    fn deep_objects_do_not_use_the_thread_stack_for_traversal() {
        let worker = std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(|| {
                let mut fields = Labels::new();
                parse_json_fields(&nested_line(4096), &mut fields);
                fields
            })
            .expect("spawn small-stack parser thread");
        let fields = worker.join().expect("parse deep objects on a small stack");
        assert!(
            fields
                == Labels::from([
                    (format!("{}level", "x_".repeat(4096)), "error".to_string()),
                    (format!("{}n", "x_".repeat(4096)), "1e3".to_string()),
                ])
        );
    }
}
