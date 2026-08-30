use super::{LabelMatcher, Labels, PipelineEvaluation, PipelineStage};

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
        let mut fields = labels.clone();
        fields.extend(initial_fields.clone());
        if !self.matchers.iter().all(|matcher| matcher.matches(labels)) {
            return None;
        }

        let mut line = line.to_string();
        for stage in &self.pipeline {
            if !stage.apply_with_timestamp(&mut line, &mut fields, timestamp_ns) {
                return None;
            }
        }

        Some(PipelineEvaluation { fields, line })
    }
}
