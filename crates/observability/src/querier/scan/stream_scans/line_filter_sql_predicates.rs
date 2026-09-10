use super::{PipelineStage, line_filter_sql_predicate};

/// The line filters at the head of `pipeline`, as SQL the block scan can push
/// into Parquet.
///
/// Every predicate here is also applied row by row afterwards, by
/// [`matching_loki_stream_entry`] over the whole pipeline. So this is a
/// pruning filter and nothing rests on it being complete -- but everything
/// rests on it never rejecting a row the pipeline would have kept. Each arm of
/// [`line_filter_sql_predicate`] is therefore either exactly its Rust
/// counterpart or, for the non-negated forms, wider than it:
///
/// - `|=` and `!=` are `str::contains`, and `LIKE '%literal%'` with the
///   wildcards escaped is the same test.
/// - `|~` and `!~` compile the pattern with the `regex` crate and ask
///   `is_match`. `regexp_like` is the same crate and the same call, on the
///   same pattern text, so the two cannot disagree -- except where
///   `DataFusion` rewrites the call into something that is not a regex at all,
///   which is what [`regex_line_filter_is_pushdown_safe`] rules out.
/// - `|>` and `!>` become `LIKE`; see [`pattern_line_filter_like_pattern`].
/// - `ip(...)` filters have no SQL form and stay in Rust.
///
/// The walk stops at the first stage that rewrites the line -- a `line_format`
/// or a parser's output -- because past that point `line` in the block is no
/// longer the text the filter sees.
///
/// [`matching_loki_stream_entry`]: crate::matching_loki_stream_entry
/// [`regex_line_filter_is_pushdown_safe`]: super::regex_line_filter_is_pushdown_safe
/// [`pattern_line_filter_like_pattern`]: super::pattern_line_filter_like_pattern
pub(crate) fn line_filter_sql_predicates(pipeline: &[PipelineStage]) -> Vec<String> {
    let mut predicates = Vec::new();
    for stage in pipeline {
        if stage.mutates_line() {
            break;
        }
        let PipelineStage::LineFilter(filter) = stage else {
            continue;
        };
        if let Some(predicate) = line_filter_sql_predicate(filter) {
            predicates.push(predicate);
        }
    }
    predicates
}
