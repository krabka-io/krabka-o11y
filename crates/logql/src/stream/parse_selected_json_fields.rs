use std::fmt;

use serde::{
    Deserialize as _,
    de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor},
};
use serde_json::value::RawValue;

use super::{JsonParserConfig, Labels, insert_raw_parsed_field, selected_json_value_to_string};
use crate::extract::JsonPathPart;

pub(crate) fn parse_selected_json_fields(
    line: &str,
    fields: &mut Labels,
    config: &JsonParserConfig,
    has_existing: impl Fn(&str) -> bool,
) {
    if line.is_empty() {
        return;
    }
    if !matches!(line.as_bytes()[0], b'"' | b'{' | b'[') {
        insert_raw_parsed_field(fields, "__error__", "JSONParserErr".into());
        return;
    }
    // Borrow complete values without normalizing number lexemes, whitespace,
    // duplicate object keys, or the text following a complete root value.
    let mut parser = serde_json::Deserializer::from_str(line);
    if let Ok(value) = <&RawValue>::deserialize(&mut parser) {
        let selected = config
            .extractions()
            .iter()
            .enumerate()
            .filter_map(|(index, extraction)| {
                let matched = extraction.evaluate(value)?;
                matched.value.map(|matched_json| SelectedField {
                    position: matched.position,
                    extraction: index,
                    extracted_text: selected_json_value_to_string(matched_json),
                })
            })
            .collect();
        SelectedFields(selected).insert_into(fields, config, &has_existing);
        return;
    }

    // EachKey stops once its requested paths match: a malformed unrequested
    // tail must not discard earlier selected values. This visitor reads only
    // the requested branches, retaining successfully decoded values on error.
    let mut selection = Selection {
        config,
        values: vec![None; config.extractions().len()],
        claimed: vec![false; config.extractions().len()],
        error_details: None,
    };
    let targets = config
        .extractions()
        .iter()
        .enumerate()
        .map(|(index, extraction)| (index, extraction.path.parts.as_slice()))
        .collect();
    let mut parser = serde_json::Deserializer::from_str(line);
    let _ = SelectedValue {
        selection: &mut selection,
        targets,
        array_element: false,
    }
    .deserialize(&mut parser);
    if let Some(details) = selection.error_details {
        insert_raw_parsed_field(fields, "__error__", "JSONParserErr".into());
        insert_raw_parsed_field(fields, "__error_details__", details);
    }
    let selected = selection
        .values
        .into_iter()
        .enumerate()
        .filter_map(|(index, selected_text)| {
            selected_text.map(|(position, extracted_text)| SelectedField {
                position,
                extraction: index,
                extracted_text,
            })
        })
        .collect();
    SelectedFields(selected).insert_into(fields, config, &has_existing);
}

/// One extracted value, at `position` in the document.
struct SelectedField<P> {
    position: P,
    /// The index of the extraction in the parser config.
    extraction: usize,
    extracted_text: String,
}

/// The values a JSON parser selected from one line.
struct SelectedFields<P>(Vec<SelectedField<P>>);

impl<P: Ord + Copy> SelectedFields<P> {
    /// Inserts the selected values in document order, then fills every
    /// requested destination that is still absent with an empty value.
    fn insert_into(
        self,
        fields: &mut Labels,
        config: &JsonParserConfig,
        has_existing: &impl Fn(&str) -> bool,
    ) {
        let Self(mut selected) = self;
        selected.sort_by_key(|field| (field.position, field.extraction));
        for field in selected {
            fields.insert(
                config.extractions()[field.extraction].destination().into(),
                field.extracted_text,
            );
        }
        for extraction in config.extractions() {
            if !has_existing(extraction.destination()) {
                insert_raw_parsed_field(fields, extraction.destination(), String::new());
            }
        }
    }
}

struct Selection<'a> {
    config: &'a JsonParserConfig,
    values: Vec<Option<(usize, String)>>,
    claimed: Vec<bool>,
    error_details: Option<String>,
}

struct SelectedValue<'a, 'p> {
    selection: &'a mut Selection<'p>,
    targets: Vec<(usize, &'p [JsonPathPart])>,
    array_element: bool,
}

impl<'de> DeserializeSeed<'de> for SelectedValue<'_, '_> {
    type Value = ();

    fn deserialize<D: serde::Deserializer<'de>>(self, parser: D) -> Result<(), D::Error> {
        if self.targets.is_empty() {
            IgnoredAny::deserialize(parser)?;
            return Ok(());
        }
        if self.array_element || self.targets.iter().any(|(_, parts)| parts.is_empty()) {
            let value = match <&RawValue>::deserialize(parser) {
                Ok(value) => value,
                Err(error) => {
                    self.selection.error_details = Some(if error.to_string().contains("string") {
                        "Value is string, but can't find closing '\"' symbol".into()
                    } else {
                        "Value looks like object, but can't find closing '}' symbol".into()
                    });
                    return Err(error);
                }
            };
            let array_position = value.get().as_ptr() as usize;
            for (index, parts) in self.targets {
                let result = self.selection.config.extractions()[index]
                    .evaluate_remaining(value, parts.len());
                if let Some(value) = result {
                    self.selection.claimed[index] = true;
                    self.selection.values[index] = value.value.map(|matched| {
                        (
                            if self.array_element {
                                array_position
                            } else {
                                value.position
                            },
                            selected_json_value_to_string(matched),
                        )
                    });
                }
            }
            return Ok(());
        }
        parser.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for SelectedValue<'_, '_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a selected JSON path")
    }

    fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<(), E> {
        Ok(())
    }
    fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<(), E> {
        Ok(())
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> {
        Ok(())
    }

    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<(), M::Error> {
        while let Some(key) = map.next_key::<String>()? {
            let targets = self
                .targets
                .iter()
                .filter_map(|(index, parts)| {
                    if self.selection.claimed[*index] {
                        return None;
                    }
                    let (JsonPathPart::Field(name), rest) = parts.split_first()? else {
                        return None;
                    };
                    (name == &key).then_some((*index, rest))
                })
                .collect();
            map.next_value_seed(SelectedValue {
                selection: self.selection,
                targets,
                array_element: false,
            })?;
            if self.selection.claimed.iter().all(|claimed| *claimed) {
                return Err(serde::de::Error::custom("selected JSON paths completed"));
            }
        }
        Ok(())
    }

    fn visit_seq<S: SeqAccess<'de>>(self, mut sequence: S) -> Result<(), S::Error> {
        let mut position = 0;
        loop {
            let targets = self
                .targets
                .iter()
                .filter_map(|(index, parts)| {
                    if self.selection.claimed[*index] {
                        return None;
                    }
                    let (JsonPathPart::Index(want), rest) = parts.split_first()? else {
                        return None;
                    };
                    (*want == position).then_some((*index, rest))
                })
                .collect::<Vec<_>>();
            let indices = targets.iter().map(|(index, _)| *index).collect::<Vec<_>>();
            if sequence
                .next_element_seed(SelectedValue {
                    selection: self.selection,
                    targets,
                    array_element: true,
                })?
                .is_none()
            {
                return Ok(());
            }
            for index in indices {
                self.selection.claimed[index] = true;
            }
            if self.selection.claimed.iter().all(|claimed| *claimed) {
                return Err(serde::de::Error::custom("selected JSON paths completed"));
            }
            position += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use crate::{Labels, parse_query};

    // Loki3.7.7 parser.go JSONExpressionParser/grafana jsonparser023329977675 EachKey.
    // Expected lexemes/duplicates were captured with the pinned Go parser.
    #[test]
    fn selected_json_preserves_numeric_and_container_text_with_first_complete_paths() {
        let labels = Labels::from([("app".into(), "api".into())]);
        let query = parse_query(r#"{app="api"} | json exponent="n", fixed="fixed", big="big", array="arr", object="obj", selected="obj.n", indexed="arr[0].n""#).unwrap();
        let line = r#"{"n":1e3,"fixed":1.00,"big":184467440737095516160,"arr":[ 1, "a\n", { "x": 2 } ],"obj":{ "x" : 1.00 },"obj":{"n":2.00}}"#;
        let result = query
            .evaluate_with_fields(&labels, line, &Labels::new())
            .unwrap();
        check!(
            (result.line.as_str(), result.fields)
                == (
                    line,
                    Labels::from([
                        ("app".into(), "api".into()),
                        ("exponent".into(), "1e3".into()),
                        ("fixed".into(), "1.00".into()),
                        ("big".into(), "184467440737095516160".into()),
                        ("array".into(), "[ 1, \"a\n\", { \"x\": 2 } ]".into()),
                        ("object".into(), r#"{ "x" : 1.00 }"#.into()),
                        ("selected".into(), "2.00".into()),
                        ("indexed".into(), String::new()),
                    ])
                )
        );
        let query =
            parse_query(r#"{app="api"} | json object="obj.n", indexed="arr[0].n", first="value""#)
                .unwrap();
        let result = query.evaluate_with_fields(&labels, r#"{"obj":{},"obj":{"n":1e3},"arr":[{}],"arr":[{"n":2}],"value":"first","value":"last"}"#, &Labels::new()).unwrap();
        check!(
            result.fields
                == Labels::from([
                    ("app".into(), "api".into()),
                    ("object".into(), "1e3".into()),
                    ("indexed".into(), String::new()),
                    ("first".into(), "first".into()),
                ])
        );
    }

    #[test]
    fn repeated_destinations_follow_source_object_and_array_callback_order() {
        let labels = Labels::from([("app".into(), "api".into())]);
        for (extractions, line, expected) in [
            (r#"value="b", value="a""#, r#"{"a":1,"b":2}"#, "2"),
            (r#"value="a", value="b""#, r#"{"a":1,"b":2}"#, "2"),
            (r#"value="b", value="a""#, r#"{"b":2,"a":1}"#, "1"),
            (r#"value="obj.n", value="obj""#, r#"{"obj":{"n":1}}"#, "1"),
            (r#"value="obj", value="obj.n""#, r#"{"obj":{"n":1}}"#, "1"),
            (
                r#"value="arr[0].n", value="arr[0]""#,
                r#"{"arr":[{"n":1}]}"#,
                r#"{"n":1}"#,
            ),
            (
                r#"value="arr[0]", value="arr[0].n""#,
                r#"{"arr":[{"n":1}]}"#,
                "1",
            ),
            (r#"value="missing", value="a""#, r#"{"a":1}"#, "1"),
            (r#"value="a", value="a""#, r#"{"a":1,"a":2}"#, "1"),
            (
                r#"value="arr[0].obj.n""#,
                r#"{"arr":[{"obj":{},"obj":{"n":1}}]}"#,
                "1",
            ),
        ] {
            let query = parse_query(&format!("{{app=\"api\"}} | json {extractions}")).unwrap();
            let result = query
                .evaluate_with_fields(&labels, line, &Labels::new())
                .unwrap();
            check!(
                result.fields
                    == Labels::from([
                        ("app".into(), "api".into()),
                        ("value".into(), expected.into()),
                    ])
            );
            // The streaming selector must preserve the same callback ordering
            // when the outer closing brace is absent after all selected values.
            let result = query
                .evaluate_with_fields(&labels, line.strip_suffix('}').unwrap(), &Labels::new())
                .unwrap();
            check!(
                result.fields
                    == Labels::from([
                        ("app".into(), "api".into()),
                        ("value".into(), expected.into()),
                    ])
            );
        }
    }

    #[test]
    fn selected_missing_fields_distinguish_empty_stream_and_metadata_labels() {
        let query = parse_query(r#"{app="api"} | json token="missing" | unpack"#).unwrap();
        let line = r#"{"token":"packed","_entry":"body"}"#;
        let base = Labels::from([
            ("app".into(), "api".into()),
            ("token".into(), String::new()),
        ]);
        let result = query
            .evaluate_with_fields(&base, line, &Labels::new())
            .unwrap();
        check!((result.line, result.fields) == ("body".into(), base));
        let base = Labels::from([("app".into(), "api".into())]);
        let metadata = Labels::from([("token".into(), String::new())]);
        let result = query.evaluate_with_fields(&base, line, &metadata).unwrap();
        check!(
            (result.line, result.fields)
                == (
                    "body".into(),
                    Labels::from([
                        ("app".into(), "api".into()),
                        ("token".into(), String::new()),
                        ("token_extracted".into(), "packed".into()),
                    ])
                )
        );
    }

    #[test]
    fn selected_json_retains_matched_values_before_malformed_unrequested_tails() {
        let labels = Labels::from([("app".into(), "api".into())]);
        for (expression, line, expected) in [
            (
                "app",
                r#"{"app":{"name":"great \"loki\""}"#,
                r#"{"name":"great \"loki\""}"#,
            ),
            ("app.name", r#"{"app":{"name":"yes""#, "yes"),
            ("app", r#"{"app":"first",BROKEN}"#, "first"),
        ] {
            let query =
                parse_query(&format!("{{app=\"api\"}} | json chosen=\"{expression}\"")).unwrap();
            let result = query
                .evaluate_with_fields(&labels, line, &Labels::new())
                .unwrap();
            check!(
                result.fields
                    == Labels::from([
                        ("app".into(), "api".into()),
                        ("chosen".into(), expected.into()),
                    ])
            );
        }
    }

    #[test]
    fn selected_json_missing_null_errors_and_metadata_keep_distinct_categories() {
        let labels = Labels::from([("app".into(), "api".into())]);
        let metadata = Labels::from([("token".into(), "metadata".into())]);
        let query = parse_query(r#"{app="api"} | json token="v", absent="missing""#).unwrap();
        let result = query
            .evaluate_with_fields(&labels, r#"{"v":null}"#, &metadata)
            .unwrap();
        check!(
            result.fields
                == Labels::from([
                    ("app".into(), "api".into()),
                    ("token".into(), "metadata".into()),
                    ("token_extracted".into(), String::new()),
                    ("absent".into(), String::new()),
                ])
        );
        let result = query
            .evaluate_with_fields(&labels, "{}", &metadata)
            .unwrap();
        check!(
            result.fields
                == Labels::from([
                    ("app".into(), "api".into()),
                    ("token".into(), "metadata".into()),
                    ("absent".into(), String::new()),
                ])
        );
        let query = parse_query(r#"{app="api"} | json chosen="v""#).unwrap();
        for (line, expected) in [
            ("", labels.clone()),
            (
                "1",
                Labels::from([
                    ("app".into(), "api".into()),
                    ("__error__".into(), "JSONParserErr".into()),
                ]),
            ),
            (
                r#"{"v":"unterminated}"#,
                Labels::from([
                    ("app".into(), "api".into()),
                    ("chosen".into(), String::new()),
                    ("__error__".into(), "JSONParserErr".into()),
                    (
                        "__error_details__".into(),
                        "Value is string, but can't find closing '\"' symbol".into(),
                    ),
                ]),
            ),
        ] {
            check!(
                query
                    .evaluate_with_fields(&labels, line, &Labels::new())
                    .unwrap()
                    .fields
                    == expected
            );
        }
    }
}
