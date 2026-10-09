use super::{
    LabelMatcher, Labels, PipelineEvaluation, PipelineStage,
    variant_metadata::{PipelineLabels, initial_pipeline_fields},
};

#[derive(Clone, Debug, PartialEq)]
pub struct StreamQuery {
    pub matchers: Vec<LabelMatcher>,
    pub pipeline: Vec<PipelineStage>,
}

impl StreamQuery {
    #[must_use]
    pub fn matches(&self, labels: &Labels, line: &str) -> bool {
        self.matches_with_fields(labels, line, &Labels::new())
    }

    #[must_use]
    pub fn matches_with_fields(
        &self,
        labels: &Labels,
        line: &str,
        initial_fields: &Labels,
    ) -> bool {
        self.evaluate_with_fields(labels, line, initial_fields)
            .is_some()
    }

    #[must_use]
    pub fn matches_with_fields_at(
        &self,
        labels: &Labels,
        line: &str,
        initial_fields: &Labels,
        timestamp_ns: i64,
    ) -> bool {
        self.evaluate_with_fields_at(labels, line, initial_fields, timestamp_ns)
            .is_some()
    }

    #[must_use]
    pub fn evaluate_with_fields(
        &self,
        labels: &Labels,
        line: &str,
        initial_fields: &Labels,
    ) -> Option<PipelineEvaluation> {
        self.evaluate_with_fields_and_timestamp(labels, line, initial_fields, None)
    }

    #[must_use]
    pub fn evaluate_with_fields_at(
        &self,
        labels: &Labels,
        line: &str,
        initial_fields: &Labels,
        timestamp_ns: i64,
    ) -> Option<PipelineEvaluation> {
        self.evaluate_with_fields_and_timestamp(labels, line, initial_fields, Some(timestamp_ns))
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(matchers = self.matchers.len(), stages = self.pipeline.len())
    )]
    pub(crate) fn evaluate_with_fields_and_timestamp(
        &self,
        labels: &Labels,
        line: &str,
        initial_fields: &Labels,
        timestamp_ns: Option<i64>,
    ) -> Option<PipelineEvaluation> {
        if !self.matchers.iter().all(|matcher| matcher.matches(labels)) {
            return None;
        }

        let (mut fields, metadata) = initial_pipeline_fields(labels, initial_fields);
        if self.pipeline.is_empty() {
            return Some(PipelineEvaluation {
                fields,
                line: line.to_string(),
            });
        }
        let mut categories = PipelineLabels::new(labels, metadata);

        let mut line = line.to_string();
        for stage in &self.pipeline {
            if matches!(stage, PipelineStage::VariantBoundary) {
                // The common extractor's complete output becomes the new stream
                // base, then its surviving structured metadata is added again.
                categories.reenter(&mut fields);
                continue;
            }
            match stage {
                PipelineStage::Parser(parser) => {
                    parser.apply_with_categories(&mut line, &mut fields, &mut categories);
                }
                PipelineStage::LabelFormat(format) => {
                    format.apply_with_assignment_tracking(
                        &line,
                        &mut fields,
                        timestamp_ns,
                        |destination| categories.record_assignment(destination),
                    );
                }
                _ => {
                    if !stage.apply_with_timestamp(&mut line, &mut fields, timestamp_ns) {
                        return None;
                    }
                }
            }
            categories.retain_metadata(&fields);
        }

        Some(PipelineEvaluation { fields, line })
    }
}
