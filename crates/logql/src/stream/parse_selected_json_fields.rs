use std::fmt;

use serde::{
    Deserialize as _,
    de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor},
};
use serde_json::value::RawValue;

use super::{JsonParserConfig, Labels, insert_raw_parsed_field, selected_json_value_to_string};
use crate::extract::JsonPathPart;

impl JsonParserConfig {
    /// Parses the JSON paths this config selects out of `line` into `fields`.
    /// A requested destination that `has_existing` reports present is not
    /// filled with an empty value when its path is absent.
    pub(crate) fn parse_selected_fields(
        &self,
        line: &str,
        fields: &mut Labels,
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
            let selected = self
                .extractions()
                .iter()
                .enumerate()
                .filter_map(|indexed_extraction| {
                    let index = indexed_extraction.0;
                    let extraction = indexed_extraction.1;
                    let matched = extraction.evaluate(value)?;
                    matched.value.map(|matched_json| SelectedField {
                        position: matched.position,
                        extraction: index,
                        extracted_text: selected_json_value_to_string(matched_json),
                    })
                })
                .collect();
            SelectedFields(selected).insert_into(fields, self, &has_existing);
            return;
        }

        // EachKey stops once its requested paths match: a malformed unrequested
        // tail must not discard earlier selected values. This visitor reads only
        // the requested branches, retaining successfully decoded values on error.
        let mut selection = Selection {
            config: self,
            values: vec![None; self.extractions().len()],
            claimed: vec![false; self.extractions().len()],
            error_details: None,
        };
        let mut targets = Vec::with_capacity(self.extractions().len());
        for index in 0..self.extractions().len() {
            let extraction = &self.extractions()[index];
            targets.push(JsonSelectionTarget {
                extraction_index: index,
                remaining_path: extraction.path.parts.as_slice(),
            });
        }
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
        let mut selected = Vec::new();
        let mut index = 0;
        for selected_text in selection.values {
            if let Some(selected_text) = selected_text {
                selected.push(SelectedField {
                    position: selected_text.position,
                    extraction: index,
                    extracted_text: selected_text.extracted_text,
                });
            }
            index += 1;
        }
        SelectedFields(selected).insert_into(fields, self, &has_existing);
    }
}

/// One extracted value, at `position` in the document.
struct SelectedField<P> {
    position: P,
    /// The index of the extraction in the parser config.
    extraction: usize,
    extracted_text: String,
}

#[derive(Clone)]
struct SelectedJsonText {
    position: usize,
    extracted_text: String,
}

struct JsonSelectionTarget<'p> {
    extraction_index: usize,
    remaining_path: &'p [JsonPathPart],
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
        selected.sort_by(|left, right| {
            left.position
                .cmp(&right.position)
                .then_with(|| left.extraction.cmp(&right.extraction))
        });
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
    values: Vec<Option<SelectedJsonText>>,
    claimed: Vec<bool>,
    error_details: Option<String>,
}

struct SelectedValue<'a, 'p> {
    selection: &'a mut Selection<'p>,
    targets: Vec<JsonSelectionTarget<'p>>,
    array_element: bool,
}

impl<'de> DeserializeSeed<'de> for SelectedValue<'_, '_> {
    type Value = ();

    fn deserialize<D: serde::Deserializer<'de>>(self, parser: D) -> Result<(), D::Error> {
        if self.targets.is_empty() {
            IgnoredAny::deserialize(parser)?;
            return Ok(());
        }
        if self.array_element
            || self
                .targets
                .iter()
                .any(|target| target.remaining_path.is_empty())
        {
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
            for target in self.targets {
                let result = self.selection.config.extractions()[target.extraction_index]
                    .evaluate_remaining(value, target.remaining_path.len());
                if let Some(value) = result {
                    self.selection.claimed[target.extraction_index] = true;
                    self.selection.values[target.extraction_index] =
                        value.value.map(|matched| SelectedJsonText {
                            position: if self.array_element {
                                array_position
                            } else {
                                value.position
                            },
                            extracted_text: selected_json_value_to_string(matched),
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
                .filter_map(|target| {
                    if self.selection.claimed[target.extraction_index] {
                        return None;
                    }
                    let JsonPathPart::Field(name) = target.remaining_path.first()? else {
                        return None;
                    };
                    let rest = &target.remaining_path[1..];
                    (name == &key).then_some(JsonSelectionTarget {
                        extraction_index: target.extraction_index,
                        remaining_path: rest,
                    })
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
                .filter_map(|target| {
                    if self.selection.claimed[target.extraction_index] {
                        return None;
                    }
                    let JsonPathPart::Index(want) = target.remaining_path.first()? else {
                        return None;
                    };
                    let rest = &target.remaining_path[1..];
                    (*want == position).then_some(JsonSelectionTarget {
                        extraction_index: target.extraction_index,
                        remaining_path: rest,
                    })
                })
                .collect::<Vec<_>>();
            let indices = targets
                .iter()
                .map(|target| target.extraction_index)
                .collect::<Vec<_>>();
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

    use crate::{
        Labels, parse_query,
        test_support::{LabelPair, labels as labels_from_pairs},
    };

    struct RepeatedDestinationCase {
        extractions: &'static str,
        line: &'static str,
        expected: &'static str,
    }

    impl RepeatedDestinationCase {
        fn new(extractions: &'static str, line: &'static str, expected: &'static str) -> Self {
            Self {
                extractions,
                line,
                expected,
            }
        }
    }

    struct SelectedJsonValueCase {
        expression: &'static str,
        line: &'static str,
        expected: &'static str,
    }

    struct SelectedJsonFieldsCase {
        line: &'static str,
        expected: Labels,
    }

    fn evaluate_selected_json(
        extractions: &str,
        labels: &Labels,
        line: &str,
    ) -> crate::stream::PipelineEvaluation {
        let query = parse_query(&format!("{{app=\"api\"}} | json {extractions}")).unwrap();
        query
            .evaluate_with_fields(labels, line, &Labels::new())
            .unwrap()
    }

    // Loki3.7.7 parser.go JSONExpressionParser/grafana jsonparser023329977675 EachKey.
    // Expected lexemes/duplicates were captured with the pinned Go parser.
    #[test]
    fn selected_json_preserves_numeric_and_container_text_with_first_complete_paths() {
        let labels = labels_from_pairs(&[LabelPair::new("app", "api")]);
        let query = parse_query(r#"{app="api"} | json exponent="n", fixed="fixed", big="big", array="arr", object="obj", selected="obj.n", indexed="arr[0].n""#).unwrap();
        let line = r#"{"n":1e3,"fixed":1.00,"big":184467440737095516160,"arr":[ 1, "a\n", { "x": 2 } ],"obj":{ "x" : 1.00 },"obj":{"n":2.00}}"#;
        let result = query
            .evaluate_with_fields(&labels, line, &Labels::new())
            .unwrap();
        check!(result.line == line);
        check!(
            result.fields
                == labels_from_pairs(&[
                    LabelPair::new("app", "api"),
                    LabelPair::new("exponent", "1e3"),
                    LabelPair::new("fixed", "1.00"),
                    LabelPair::new("big", "184467440737095516160"),
                    LabelPair::new("array", "[ 1, \"a\n\", { \"x\": 2 } ]"),
                    LabelPair::new("object", r#"{ "x" : 1.00 }"#),
                    LabelPair::new("selected", "2.00"),
                    LabelPair::new("indexed", ""),
                ])
        );
        let query =
            parse_query(r#"{app="api"} | json object="obj.n", indexed="arr[0].n", first="value""#)
                .unwrap();
        let result = query.evaluate_with_fields(&labels, r#"{"obj":{},"obj":{"n":1e3},"arr":[{}],"arr":[{"n":2}],"value":"first","value":"last"}"#, &Labels::new()).unwrap();
        check!(
            result.fields
                == labels_from_pairs(&[
                    LabelPair::new("app", "api"),
                    LabelPair::new("object", "1e3"),
                    LabelPair::new("indexed", ""),
                    LabelPair::new("first", "first"),
                ])
        );
    }

    #[test]
    fn repeated_destinations_follow_source_object_and_array_callback_order() {
        let labels = labels_from_pairs(&[LabelPair::new("app", "api")]);
        for case in [
            RepeatedDestinationCase::new(r#"value="b", value="a""#, r#"{"a":1,"b":2}"#, "2"),
            RepeatedDestinationCase::new(r#"value="a", value="b""#, r#"{"a":1,"b":2}"#, "2"),
            RepeatedDestinationCase::new(r#"value="b", value="a""#, r#"{"b":2,"a":1}"#, "1"),
            RepeatedDestinationCase::new(
                r#"value="obj.n", value="obj""#,
                r#"{"obj":{"n":1}}"#,
                "1",
            ),
            RepeatedDestinationCase::new(
                r#"value="obj", value="obj.n""#,
                r#"{"obj":{"n":1}}"#,
                "1",
            ),
            RepeatedDestinationCase::new(
                r#"value="arr[0].n", value="arr[0]""#,
                r#"{"arr":[{"n":1}]}"#,
                r#"{"n":1}"#,
            ),
            RepeatedDestinationCase::new(
                r#"value="arr[0]", value="arr[0].n""#,
                r#"{"arr":[{"n":1}]}"#,
                "1",
            ),
            RepeatedDestinationCase::new(r#"value="missing", value="a""#, r#"{"a":1}"#, "1"),
            RepeatedDestinationCase::new(r#"value="a", value="a""#, r#"{"a":1,"a":2}"#, "1"),
            RepeatedDestinationCase::new(
                r#"value="arr[0].obj.n""#,
                r#"{"arr":[{"obj":{},"obj":{"n":1}}]}"#,
                "1",
            ),
        ] {
            let result = evaluate_selected_json(case.extractions, &labels, case.line);
            check!(
                result.fields
                    == labels_from_pairs(&[
                        LabelPair::new("app", "api"),
                        LabelPair::new("value", case.expected),
                    ])
            );
            // The streaming selector must preserve the same callback ordering
            // when the outer closing brace is absent after all selected values.
            let result = evaluate_selected_json(
                case.extractions,
                &labels,
                case.line.strip_suffix('}').unwrap(),
            );
            check!(
                result.fields
                    == labels_from_pairs(&[
                        LabelPair::new("app", "api"),
                        LabelPair::new("value", case.expected),
                    ])
            );
        }
    }

    #[test]
    fn selected_missing_fields_distinguish_empty_stream_and_metadata_labels() {
        let query = parse_query(r#"{app="api"} | json token="missing" | unpack"#).unwrap();
        let line = r#"{"token":"packed","_entry":"body"}"#;
        let base = labels_from_pairs(&[LabelPair::new("app", "api"), LabelPair::new("token", "")]);
        let result = query
            .evaluate_with_fields(&base, line, &Labels::new())
            .unwrap();
        check!(result.line == "body");
        check!(result.fields == base);
        let base = labels_from_pairs(&[LabelPair::new("app", "api")]);
        let metadata = labels_from_pairs(&[LabelPair::new("token", "")]);
        let result = query.evaluate_with_fields(&base, line, &metadata).unwrap();
        check!(result.line == "body");
        check!(
            result.fields
                == labels_from_pairs(&[
                    LabelPair::new("app", "api"),
                    LabelPair::new("token", ""),
                    LabelPair::new("token_extracted", "packed"),
                ])
        );
    }

    #[test]
    fn selected_json_retains_matched_values_before_malformed_unrequested_tails() {
        let labels = labels_from_pairs(&[LabelPair::new("app", "api")]);
        for case in [
            SelectedJsonValueCase {
                expression: "app",
                line: r#"{"app":{"name":"great \"loki\""}"#,
                expected: r#"{"name":"great \"loki\""}"#,
            },
            SelectedJsonValueCase {
                expression: "app.name",
                line: r#"{"app":{"name":"yes""#,
                expected: "yes",
            },
            SelectedJsonValueCase {
                expression: "app",
                line: r#"{"app":"first",BROKEN}"#,
                expected: "first",
            },
        ] {
            let result = evaluate_selected_json(
                &format!("chosen=\"{}\"", case.expression),
                &labels,
                case.line,
            );
            check!(
                result.fields
                    == labels_from_pairs(&[
                        LabelPair::new("app", "api"),
                        LabelPair::new("chosen", case.expected),
                    ])
            );
        }
    }

    #[test]
    fn selected_json_missing_null_errors_and_metadata_keep_distinct_categories() {
        let labels = labels_from_pairs(&[LabelPair::new("app", "api")]);
        let metadata = labels_from_pairs(&[LabelPair::new("token", "metadata")]);
        let query = parse_query(r#"{app="api"} | json token="v", absent="missing""#).unwrap();
        let result = query
            .evaluate_with_fields(&labels, r#"{"v":null}"#, &metadata)
            .unwrap();
        check!(
            result.fields
                == labels_from_pairs(&[
                    LabelPair::new("app", "api"),
                    LabelPair::new("token", "metadata"),
                    LabelPair::new("token_extracted", ""),
                    LabelPair::new("absent", ""),
                ])
        );
        let result = query
            .evaluate_with_fields(&labels, "{}", &metadata)
            .unwrap();
        check!(
            result.fields
                == labels_from_pairs(&[
                    LabelPair::new("app", "api"),
                    LabelPair::new("token", "metadata"),
                    LabelPair::new("absent", ""),
                ])
        );
        let query = parse_query(r#"{app="api"} | json chosen="v""#).unwrap();
        for case in [
            SelectedJsonFieldsCase {
                line: "",
                expected: labels.clone(),
            },
            SelectedJsonFieldsCase {
                line: "1",
                expected: labels_from_pairs(&[
                    LabelPair::new("app", "api"),
                    LabelPair::new("__error__", "JSONParserErr"),
                ]),
            },
            SelectedJsonFieldsCase {
                line: r#"{"v":"unterminated}"#,
                expected: labels_from_pairs(&[
                    LabelPair::new("app", "api"),
                    LabelPair::new("chosen", ""),
                    LabelPair::new("__error__", "JSONParserErr"),
                    LabelPair::new(
                        "__error_details__",
                        "Value is string, but can't find closing '\"' symbol",
                    ),
                ]),
            },
        ] {
            check!(
                query
                    .evaluate_with_fields(&labels, case.line, &Labels::new())
                    .unwrap()
                    .fields
                    == case.expected
            );
        }
    }
}
