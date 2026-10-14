use std::collections::BTreeSet;

use super::{Labels, ParserStage};

// Loki 3.7.7 log/labels.go Add uses the original stream base for collision
// names. Track categories separately so common drop/keep/label_format stages
// cannot resurrect metadata that is absent or has become a parsed label.
pub(super) struct InitialPipelineFields {
    pub(super) fields: Labels,
    pub(super) metadata: Labels,
}

pub(super) fn initial_pipeline_fields(labels: &Labels, initial: &Labels) -> InitialPipelineFields {
    let mut fields = labels.clone();
    let mut metadata = Labels::new();
    for entry in initial {
        let name = entry.0;
        let value = entry.1;
        if matches!(name.as_str(), "__error__" | "__error_details__") {
            fields.insert(name.to_owned(), value.to_owned());
            continue;
        }
        insert_extracted_metadata(
            &mut fields,
            &mut metadata,
            name,
            value,
            labels.get(name).is_some_and(|value| !value.is_empty()),
        );
    }
    InitialPipelineFields { fields, metadata }
}

// LabelsBuilder keeps the immutable stream base apart from structured metadata
// and parser hints. The hints survive drop/keep, and reset for a new extractor.
pub(super) struct PipelineLabels {
    base: Labels,
    metadata: Labels,
    extracted: BTreeSet<String>,
}

impl PipelineLabels {
    pub(super) fn new(base: &Labels, metadata: Labels) -> Self {
        Self {
            base: base.clone(),
            metadata,
            extracted: BTreeSet::new(),
        }
    }

    pub(super) fn reenter(&mut self, fields: &mut Labels) {
        self.base = fields.clone();
        self.extracted.clear();
        let mut metadata = Labels::new();
        for entry in &self.metadata {
            let name = entry.0;
            let value = entry.1;
            insert_extracted_metadata(
                fields,
                &mut metadata,
                name,
                value,
                self.base.get(name).is_some_and(|value| !value.is_empty()),
            );
        }
        self.metadata = metadata;
    }

    pub(super) fn has_collision(&self, name: &str) -> bool {
        self.base.get(name).is_some_and(|value| !value.is_empty())
            || self.metadata.contains_key(name)
    }

    pub(super) fn has_metadata(&self, name: &str) -> bool {
        self.metadata.contains_key(name)
    }

    pub(super) fn has_extracted(&self, name: &str) -> bool {
        self.extracted.contains(name)
    }

    fn collision_name(&self, name: &str) -> String {
        extracted_name(name, self.has_collision(name))
    }

    pub(super) fn insert_parser_fields(
        &mut self,
        fields: &mut Labels,
        parsed: Labels,
        parser: &ParserStage,
    ) {
        for entry in parsed {
            let name = entry.0;
            let value = entry.1;
            if matches!(name.as_str(), "__error__" | "__error_details__") {
                fields.insert(name, value);
                continue;
            }
            // Unpack resolves raw key collisions before sanitizing names.
            // Its buffered Set can intentionally replace a sanitized base key.
            let destination = if matches!(parser, ParserStage::Unpack) {
                name.clone()
            } else {
                self.collision_name(&name)
            };
            // Expression JSON parsers Set their explicitly selected field;
            // expression logfmt only skips duplicates of collision names.
            let replaces = matches!(parser, ParserStage::JsonSelected(_) | ParserStage::Unpack)
                || (matches!(parser, ParserStage::LogfmtSelected(_)) && destination == name);
            let first = self.extracted.insert(destination.clone());
            if replaces || first {
                self.metadata.remove(&destination);
                fields.insert(destination, value);
            }
        }
    }

    pub(super) fn record_assignment(&mut self, destination: &str) {
        self.metadata.remove(destination);
        self.extracted.insert(destination.to_string());
    }

    pub(super) fn retain_metadata(&mut self, fields: &Labels) {
        self.metadata.retain(|name, _| fields.contains_key(name));
    }
}

fn insert_extracted_metadata(
    fields: &mut Labels,
    metadata: &mut Labels,
    name: &str,
    value: &str,
    collides: bool,
) {
    let name = extracted_name(name, collides);
    fields.insert(name.clone(), value.to_owned());
    metadata.insert(name, value.to_owned());
}

fn extracted_name(name: &str, collision: bool) -> String {
    if collision {
        format!("{name}_extracted")
    } else {
        name.to_string()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::{Labels, PipelineStage, parse_query};

    struct LabelEntry {
        name: &'static str,
        value: &'static str,
    }

    impl LabelEntry {
        fn new(name: &'static str, value: &'static str) -> Self {
            Self { name, value }
        }
    }

    fn fixture_labels<const N: usize>(entries: [LabelEntry; N]) -> Labels {
        let mut labels = Labels::new();
        for entry in entries {
            labels.insert(entry.name.to_owned(), entry.value.to_owned());
        }
        labels
    }

    struct PipelineFieldsCase {
        pipeline: &'static str,
        expected: Labels,
    }

    struct PipelineLineCase {
        pipeline: &'static str,
        line: &'static str,
    }

    #[test]
    fn variant_boundary_reenters_surviving_metadata_with_source_collision_names() {
        let labels = fixture_labels([
            LabelEntry::new("app", "api"),
            LabelEntry::new("collision", "stream"),
        ]);
        let metadata = fixture_labels([
            LabelEntry::new("token", "blue"),
            LabelEntry::new("collision", "meta"),
        ]);
        let mut query = parse_query(r#"{app="api"} | logfmt"#).unwrap();
        query.pipeline.push(PipelineStage::VariantBoundary);
        let result = query
            .evaluate_with_fields_at(&labels, "token=parsed", &metadata, 123)
            .unwrap();
        // Common parsing creates token_extracted=parsed. Re-entering metadata
        // replaces that base value, as LabelsBuilder.Add/Set does upstream.
        assert!(
            result.fields
                == fixture_labels([
                    LabelEntry::new("app", "api"),
                    LabelEntry::new("collision", "stream"),
                    LabelEntry::new("collision_extracted", "meta"),
                    LabelEntry::new("collision_extracted_extracted", "meta"),
                    LabelEntry::new("token", "blue"),
                    LabelEntry::new("token_extracted", "blue"),
                ])
        );
    }

    #[test]
    fn common_drop_keep_and_label_format_change_the_surviving_metadata_category() {
        let labels = fixture_labels([LabelEntry::new("app", "api")]);
        let metadata = fixture_labels([
            LabelEntry::new("token", "blue"),
            LabelEntry::new("spare", "red"),
        ]);
        for case in [
            PipelineFieldsCase {
                pipeline: "drop token,spare",
                expected: fixture_labels([LabelEntry::new("app", "api")]),
            },
            PipelineFieldsCase {
                pipeline: "keep app,token",
                expected: fixture_labels([
                    LabelEntry::new("app", "api"),
                    LabelEntry::new("token", "blue"),
                    LabelEntry::new("token_extracted", "blue"),
                ]),
            },
            PipelineFieldsCase {
                pipeline: r#"label_format token="{{.token}}" | drop spare"#,
                expected: fixture_labels([
                    LabelEntry::new("app", "api"),
                    LabelEntry::new("token", "blue"),
                ]),
            },
            PipelineFieldsCase {
                pipeline: "label_format renamed=token | drop spare",
                expected: fixture_labels([
                    LabelEntry::new("app", "api"),
                    LabelEntry::new("renamed", "blue"),
                ]),
            },
        ] {
            let mut query = parse_query(&format!(r#"{{app="api"}} | {}"#, case.pipeline)).unwrap();
            query.pipeline.push(PipelineStage::VariantBoundary);
            let result = query
                .evaluate_with_fields_at(&labels, "line", &metadata, 123)
                .unwrap();
            assert!(result.fields == case.expected, "{}", case.pipeline);
        }
        let ordinary = parse_query(r#"{app="api"}"#).unwrap();
        let fields = ordinary
            .evaluate_with_fields_at(&labels, "line", &metadata, 123)
            .unwrap()
            .fields;
        assert!(
            fields
                == fixture_labels([
                    LabelEntry::new("app", "api"),
                    LabelEntry::new("token", "blue"),
                    LabelEntry::new("spare", "red")
                ])
        );
    }

    #[test]
    fn common_filters_and_variant_filters_observe_their_own_metadata_boundary() {
        let labels = fixture_labels([LabelEntry::new("app", "api")]);
        let metadata = fixture_labels([LabelEntry::new("token", "blue")]);
        let mut common = parse_query(r#"{app="api"} | token="blue""#).unwrap();
        common.pipeline.push(PipelineStage::VariantBoundary);
        common.pipeline.extend(
            parse_query(r#"{app="ignored"} | token_extracted="blue""#)
                .unwrap()
                .pipeline,
        );
        assert!(
            common
                .evaluate_with_fields_at(&labels, "line", &metadata, 123)
                .is_some()
        );
        let mut rejected = parse_query(r#"{app="api"} | token="red""#).unwrap();
        rejected.pipeline.push(PipelineStage::VariantBoundary);
        assert!(
            rejected
                .evaluate_with_fields_at(&labels, "line", &metadata, 123)
                .is_none()
        );
        common.pipeline.extend(
            parse_query(r#"{app="ignored"} | token_extracted="red""#)
                .unwrap()
                .pipeline,
        );
        assert!(
            common
                .evaluate_with_fields_at(&labels, "line", &metadata, 123)
                .is_none()
        );
    }

    #[test]
    fn parser_categories_preserve_first_extraction_but_replace_base_and_metadata_collisions() {
        let app = fixture_labels([LabelEntry::new("app", "api")]);
        let repeated =
            parse_query(r#"{app="api"} | logfmt | line_format "token=second" | logfmt"#).unwrap();
        let result = repeated
            .evaluate_with_fields_at(&app, "token=first", &Labels::new(), 123)
            .unwrap();
        assert!(
            result.fields
                == fixture_labels([
                    LabelEntry::new("app", "api"),
                    LabelEntry::new("token", "first")
                ])
        );

        let base = fixture_labels([
            LabelEntry::new("app", "api"),
            LabelEntry::new("token", "stream"),
            LabelEntry::new("token_extracted", "oldbase"),
        ]);
        let parser = parse_query(r#"{app="api"} | logfmt"#).unwrap();
        let result = parser
            .evaluate_with_fields_at(&base, "token=parsed", &Labels::new(), 123)
            .unwrap();
        assert!(
            result.fields
                == fixture_labels([
                    LabelEntry::new("app", "api"),
                    LabelEntry::new("token", "stream"),
                    LabelEntry::new("token_extracted", "parsed")
                ])
        );

        let metadata = fixture_labels([LabelEntry::new("token", "blue")]);
        let ordinary = parser
            .evaluate_with_fields_at(&app, "token=parsed", &metadata, 123)
            .unwrap();
        assert!(
            ordinary.fields
                == fixture_labels([
                    LabelEntry::new("app", "api"),
                    LabelEntry::new("token", "blue"),
                    LabelEntry::new("token_extracted", "parsed")
                ])
        );
        let mut variant = parse_query(r#"{app="api"}"#).unwrap();
        variant.pipeline.push(PipelineStage::VariantBoundary);
        variant.pipeline.extend(parser.pipeline);
        let result = variant
            .evaluate_with_fields_at(&app, "token=parsed", &metadata, 123)
            .unwrap();
        assert!(result.fields == ordinary.fields);

        // Dropping a parsed label does not reset ParserHints.RecordExtracted.
        let dropped = parse_query(
            r#"{app="api"} | logfmt | drop token | line_format "token=second" | logfmt"#,
        )
        .unwrap();
        assert!(
            dropped
                .evaluate_with_fields_at(&app, "token=first", &Labels::new(), 123)
                .unwrap()
                .fields
                == app
        );
    }

    #[test]
    fn every_parser_uses_the_shared_category_aware_collision_path() {
        let base = fixture_labels([
            LabelEntry::new("app", "api"),
            LabelEntry::new("token", "stream"),
            LabelEntry::new("token_extracted", "oldbase"),
        ]);
        let expected = fixture_labels([
            LabelEntry::new("app", "api"),
            LabelEntry::new("token", "stream"),
            LabelEntry::new("token_extracted", "parsed"),
        ]);
        for case in [
            PipelineLineCase {
                pipeline: "json",
                line: r#"{"token":"parsed"}"#,
            },
            PipelineLineCase {
                pipeline: r#"json token="token""#,
                line: r#"{"token":"parsed"}"#,
            },
            PipelineLineCase {
                pipeline: "logfmt",
                line: "token=parsed",
            },
            PipelineLineCase {
                pipeline: "logfmt --strict",
                line: "token=parsed",
            },
            PipelineLineCase {
                pipeline: r#"logfmt token="token""#,
                line: "token=parsed",
            },
            PipelineLineCase {
                pipeline: r#"pattern "<token>""#,
                line: "parsed",
            },
            PipelineLineCase {
                pipeline: r#"regexp "(?P<token>.*)""#,
                line: "parsed",
            },
            PipelineLineCase {
                pipeline: "unpack",
                line: r#"{"token":"parsed","_entry":"body"}"#,
            },
        ] {
            let query = parse_query(&format!(r#"{{app="api"}} | {}"#, case.pipeline)).unwrap();
            let result = query
                .evaluate_with_fields_at(&base, case.line, &Labels::new(), 123)
                .unwrap();
            assert!(result.fields == expected, "{}", case.pipeline);
        }
    }

    #[test]
    fn missing_rename_is_noop_and_self_rename_removes_the_source_category() {
        let labels = fixture_labels([LabelEntry::new("app", "api")]);
        let metadata = fixture_labels([LabelEntry::new("token", "blue")]);
        let mut query = parse_query(r#"{app="api"} | label_format token=missing"#).unwrap();
        let ordinary = query
            .evaluate_with_fields_at(&labels, "line", &metadata, 123)
            .unwrap();
        assert!(
            ordinary.fields
                == fixture_labels([
                    LabelEntry::new("app", "api"),
                    LabelEntry::new("token", "blue")
                ])
        );
        query.pipeline.push(PipelineStage::VariantBoundary);
        let variant = query
            .evaluate_with_fields_at(&labels, "line", &metadata, 123)
            .unwrap();
        assert!(
            variant.fields
                == fixture_labels([
                    LabelEntry::new("app", "api"),
                    LabelEntry::new("token", "blue"),
                    LabelEntry::new("token_extracted", "blue")
                ])
        );
        let mut same = parse_query(r#"{app="api"} | label_format token=token"#).unwrap();
        same.pipeline.push(PipelineStage::VariantBoundary);
        assert!(
            same.evaluate_with_fields_at(&labels, "line", &metadata, 123)
                .unwrap()
                .fields
                == labels
        );
    }

    #[test]
    fn standalone_parser_stage_keeps_base_collision_behavior_for_direct_callers() {
        let query = parse_query(r#"{app="api"} | logfmt"#).unwrap();
        let mut fields = fixture_labels([
            LabelEntry::new("token", "stream"),
            LabelEntry::new("token_extracted", "oldbase"),
        ]);
        let mut line = "token=parsed".to_string();
        assert!(query.pipeline[0].apply(&mut line, &mut fields));
        assert!(
            fields
                == fixture_labels([
                    LabelEntry::new("token", "stream"),
                    LabelEntry::new("token_extracted", "parsed")
                ])
        );
    }
}
