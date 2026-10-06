use std::collections::BTreeSet;

use super::{Labels, ParserStage};

// Loki 3.7.7 log/labels.go Add uses the original stream base for collision
// names. Track categories separately so common drop/keep/label_format stages
// cannot resurrect metadata that is absent or has become a parsed label.
pub(super) fn initial_pipeline_fields(labels: &Labels, initial: &Labels) -> (Labels, Labels) {
    let mut fields = labels.clone();
    let mut metadata = Labels::new();
    for (name, value) in initial {
        if matches!(name.as_str(), "__error__" | "__error_details__") {
            fields.insert(name.clone(), value.clone());
            continue;
        }
        let name = if labels.get(name).is_some_and(|value| !value.is_empty()) {
            format!("{name}_extracted")
        } else {
            name.clone()
        };
        fields.insert(name.clone(), value.clone());
        metadata.insert(name, value.clone());
    }
    (fields, metadata)
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
        for (name, value) in &self.metadata {
            let name = if self.base.get(name).is_some_and(|value| !value.is_empty()) {
                format!("{name}_extracted")
            } else {
                name.clone()
            };
            fields.insert(name.clone(), value.clone());
            metadata.insert(name, value.clone());
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
        if self.has_collision(name) {
            format!("{name}_extracted")
        } else {
            name.to_string()
        }
    }

    pub(super) fn insert_parser_fields(
        &mut self,
        fields: &mut Labels,
        parsed: Labels,
        parser: &ParserStage,
    ) {
        for (name, value) in parsed {
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

#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::{Labels, PipelineStage, parse_query};

    #[test]
    fn variant_boundary_reenters_surviving_metadata_with_source_collision_names() {
        let labels = Labels::from([
            ("app".into(), "api".into()),
            ("collision".into(), "stream".into()),
        ]);
        let metadata = Labels::from([
            ("token".into(), "blue".into()),
            ("collision".into(), "meta".into()),
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
                == Labels::from([
                    ("app".into(), "api".into()),
                    ("collision".into(), "stream".into()),
                    ("collision_extracted".into(), "meta".into()),
                    ("collision_extracted_extracted".into(), "meta".into()),
                    ("token".into(), "blue".into()),
                    ("token_extracted".into(), "blue".into()),
                ])
        );
    }

    #[test]
    fn common_drop_keep_and_label_format_change_the_surviving_metadata_category() {
        let labels = Labels::from([("app".into(), "api".into())]);
        let metadata = Labels::from([
            ("token".into(), "blue".into()),
            ("spare".into(), "red".into()),
        ]);
        for (pipeline, expected) in [
            (
                "drop token,spare",
                Labels::from([("app".into(), "api".into())]),
            ),
            (
                "keep app,token",
                Labels::from([
                    ("app".into(), "api".into()),
                    ("token".into(), "blue".into()),
                    ("token_extracted".into(), "blue".into()),
                ]),
            ),
            (
                r#"label_format token="{{.token}}" | drop spare"#,
                Labels::from([
                    ("app".into(), "api".into()),
                    ("token".into(), "blue".into()),
                ]),
            ),
            (
                "label_format renamed=token | drop spare",
                Labels::from([
                    ("app".into(), "api".into()),
                    ("renamed".into(), "blue".into()),
                ]),
            ),
        ] {
            let mut query = parse_query(&format!(r#"{{app="api"}} | {pipeline}"#)).unwrap();
            query.pipeline.push(PipelineStage::VariantBoundary);
            let result = query
                .evaluate_with_fields_at(&labels, "line", &metadata, 123)
                .unwrap();
            assert!(result.fields == expected, "{pipeline}");
        }
        let ordinary = parse_query(r#"{app="api"}"#).unwrap();
        let fields = ordinary
            .evaluate_with_fields_at(&labels, "line", &metadata, 123)
            .unwrap()
            .fields;
        assert!(
            fields
                == Labels::from([
                    ("app".into(), "api".into()),
                    ("token".into(), "blue".into()),
                    ("spare".into(), "red".into())
                ])
        );
    }

    #[test]
    fn common_filters_and_variant_filters_observe_their_own_metadata_boundary() {
        let labels = Labels::from([("app".into(), "api".into())]);
        let metadata = Labels::from([("token".into(), "blue".into())]);
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
        let app = Labels::from([("app".into(), "api".into())]);
        let repeated =
            parse_query(r#"{app="api"} | logfmt | line_format "token=second" | logfmt"#).unwrap();
        let result = repeated
            .evaluate_with_fields_at(&app, "token=first", &Labels::new(), 123)
            .unwrap();
        assert!(
            result.fields
                == Labels::from([
                    ("app".into(), "api".into()),
                    ("token".into(), "first".into())
                ])
        );

        let base = Labels::from([
            ("app".into(), "api".into()),
            ("token".into(), "stream".into()),
            ("token_extracted".into(), "oldbase".into()),
        ]);
        let parser = parse_query(r#"{app="api"} | logfmt"#).unwrap();
        let result = parser
            .evaluate_with_fields_at(&base, "token=parsed", &Labels::new(), 123)
            .unwrap();
        assert!(
            result.fields
                == Labels::from([
                    ("app".into(), "api".into()),
                    ("token".into(), "stream".into()),
                    ("token_extracted".into(), "parsed".into())
                ])
        );

        let metadata = Labels::from([("token".into(), "blue".into())]);
        let ordinary = parser
            .evaluate_with_fields_at(&app, "token=parsed", &metadata, 123)
            .unwrap();
        assert!(
            ordinary.fields
                == Labels::from([
                    ("app".into(), "api".into()),
                    ("token".into(), "blue".into()),
                    ("token_extracted".into(), "parsed".into())
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
        let base = Labels::from([
            ("app".into(), "api".into()),
            ("token".into(), "stream".into()),
            ("token_extracted".into(), "oldbase".into()),
        ]);
        let expected = Labels::from([
            ("app".into(), "api".into()),
            ("token".into(), "stream".into()),
            ("token_extracted".into(), "parsed".into()),
        ]);
        for (pipeline, line) in [
            ("json", r#"{"token":"parsed"}"#),
            (r#"json token="token""#, r#"{"token":"parsed"}"#),
            ("logfmt", "token=parsed"),
            ("logfmt --strict", "token=parsed"),
            (r#"logfmt token="token""#, "token=parsed"),
            (r#"pattern "<token>""#, "parsed"),
            (r#"regexp "(?P<token>.*)""#, "parsed"),
            ("unpack", r#"{"token":"parsed","_entry":"body"}"#),
        ] {
            let query = parse_query(&format!(r#"{{app="api"}} | {pipeline}"#)).unwrap();
            let result = query
                .evaluate_with_fields_at(&base, line, &Labels::new(), 123)
                .unwrap();
            assert!(result.fields == expected, "{pipeline}");
        }
    }

    #[test]
    fn missing_rename_is_noop_and_self_rename_removes_the_source_category() {
        let labels = Labels::from([("app".into(), "api".into())]);
        let metadata = Labels::from([("token".into(), "blue".into())]);
        let mut query = parse_query(r#"{app="api"} | label_format token=missing"#).unwrap();
        let ordinary = query
            .evaluate_with_fields_at(&labels, "line", &metadata, 123)
            .unwrap();
        assert!(
            ordinary.fields
                == Labels::from([
                    ("app".into(), "api".into()),
                    ("token".into(), "blue".into())
                ])
        );
        query.pipeline.push(PipelineStage::VariantBoundary);
        let variant = query
            .evaluate_with_fields_at(&labels, "line", &metadata, 123)
            .unwrap();
        assert!(
            variant.fields
                == Labels::from([
                    ("app".into(), "api".into()),
                    ("token".into(), "blue".into()),
                    ("token_extracted".into(), "blue".into())
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
        let mut fields = Labels::from([
            ("token".into(), "stream".into()),
            ("token_extracted".into(), "oldbase".into()),
        ]);
        let mut line = "token=parsed".to_string();
        assert!(query.pipeline[0].apply(&mut line, &mut fields));
        assert!(
            fields
                == Labels::from([
                    ("token".into(), "stream".into()),
                    ("token_extracted".into(), "parsed".into())
                ])
        );
    }
}
