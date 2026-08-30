use super::prelude::{
    BTreeMap, BTreeSet, BlockIndex, CompactionFrontier, HeaderMap, LabelIndex, Labels,
    PrometheusRulesFilters, QuerierState, StreamPlan, StreamQuery, TimeRange, check,
    count_loki_metric_result_hot_tail_samples, format_metric_range_selector, json,
    log_line_pattern, parse_metric_query, pattern_value_is_variable,
};

// === split-modules: generated submodules ===
mod a_patterns_scan_keeps_the_window_half_open;
mod count_loki_metric_result_hot_tail_samples_returns_zero_when_nothing_matches;
mod format_metric_range_selector_signs_negative_offset;
mod json_log_lines_collapse_to_a_single_templated_pattern;
mod json_log_pattern_templatizes_ids_and_numbers_but_keeps_constants;
mod json_message_field_templatizes_embedded_variables;
mod non_json_lines_still_use_logfmt_mining;
mod pattern_value_variable_classification;
mod prometheus_rules_filters_parse_all_supported_axes;
