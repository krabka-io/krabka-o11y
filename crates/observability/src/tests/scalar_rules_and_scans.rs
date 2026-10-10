use super::prelude::{
    Arc, ArrayRef, BTreeMap, BTreeSet, DataType, Field, LabelIndex, Labels, LokiStreamEncoding,
    MetricValue, PipelineStage, QueryRow, RecordBatch, Schema, SessionContext, StreamPlan,
    StringArray, TimeRange, append_matching_log_row, check, json, line_filter_sql_predicates,
    loki_streams_response, parse_query,
};
use crate::SeriesFingerprint;

mod a_label_format_stage_parses_a_label_whatever_it_overwrote;
mod a_log_row_is_appended_only_when_the_plan_asked_for_it;
mod a_parser_stage_fills_the_parsed_bucket_and_not_the_metadata_one;
mod a_pushed_line_filter_predicate_selects_the_same_lines_as_the_rust_filter;
mod a_scalar_comparison_answers_every_operator_from_both_sides;
mod hot_tail_lines_are_matched_off_one_record_at_a_time;
mod paging_rule_groups_resumes_after_the_token_it_handed_back;
mod structured_metadata_splits_a_stream_unless_the_request_categorizes_labels;

// A label index that holds the one series `{app="api"}`, and a plan that
// reads that series with a query.
struct ApiSeriesPlan {
    label_index: LabelIndex,
    api: SeriesFingerprint,
    plan: StreamPlan,
}

impl ApiSeriesPlan {
    fn new(query: &str) -> Self {
        let mut label_index = LabelIndex::default();
        let mut labels = Labels::default();
        labels.insert("app".to_string(), "api".to_string());
        let api = label_index.insert_series("tenant", labels);
        let plan = StreamPlan {
            tenant: "tenant".to_string(),
            time_range: TimeRange::new(0, 100).expect("a valid range"),
            query: parse_query(query).expect("the query parses"),
            fingerprints: [api].into_iter().collect(),
            blocks: Vec::new(),
        };
        Self {
            label_index,
            api,
            plan,
        }
    }
}
