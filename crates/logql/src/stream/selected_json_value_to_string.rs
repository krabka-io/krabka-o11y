use serde_json::value::RawValue;

pub(crate) fn selected_json_value_to_string(value: &RawValue) -> String {
    let text = value.get();
    match text.as_bytes().first() {
        Some(b'{') => text.to_string(),
        Some(b'"') => serde_json::from_str::<String>(text)
            .unwrap_or_default()
            .replace('\u{fffd}', " "),
        Some(b'[') => {
            // Loki's unescapeJSONString receives the entire array token,
            // retaining its whitespace but unescaping embedded string escapes.
            let mut quoted = String::from("\"");
            let mut escaped = false;
            for character in text.chars() {
                if escaped {
                    quoted.push(character);
                    escaped = false;
                } else {
                    match character {
                        '\\' => {
                            quoted.push(character);
                            escaped = true;
                        }
                        '"' => quoted.push_str("\\\""),
                        '\n' => quoted.push_str("\\n"),
                        '\r' => quoted.push_str("\\r"),
                        '\t' => quoted.push_str("\\t"),
                        _ => quoted.push(character),
                    }
                }
            }
            quoted.push('"');
            serde_json::from_str::<String>(&quoted)
                .unwrap_or_default()
                .replace('\u{fffd}', " ")
        }
        _ if text == "null" => String::new(),
        _ => text.to_string(),
    }
}
