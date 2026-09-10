use super::{
    LineFilter, LineFilterOp, pattern_line_filter_like_pattern, regex_line_filter_is_pushdown_safe,
    sql_like_pattern_literal, sql_regex_pattern_literal,
};

/// One line filter as a SQL predicate over `line`, where it has one.
///
/// `None` where the filter has no SQL form the scan can be trusted with: an
/// `ip(...)` matcher, a regex `DataFusion` may rewrite into a `LIKE` that
/// means something else, or a `|>` pattern with no literal run in it.
pub(crate) fn line_filter_sql_predicate(filter: &LineFilter) -> Option<String> {
    if filter.is_ip_matcher() {
        return None;
    }
    match filter.op {
        LineFilterOp::Contains => Some(format!(
            "line like '%{}%'",
            sql_like_pattern_literal(&filter.pattern)
        )),
        LineFilterOp::NotContains => Some(format!(
            "line not like '%{}%'",
            sql_like_pattern_literal(&filter.pattern)
        )),
        LineFilterOp::Regex => regex_line_filter_is_pushdown_safe(&filter.pattern).then(|| {
            format!(
                "regexp_like(line, '{}')",
                sql_regex_pattern_literal(&filter.pattern)
            )
        }),
        LineFilterOp::NotRegex => regex_line_filter_is_pushdown_safe(&filter.pattern).then(|| {
            format!(
                "not regexp_like(line, '{}')",
                sql_regex_pattern_literal(&filter.pattern)
            )
        }),
        LineFilterOp::Pattern => pattern_line_filter_like_pattern(&filter.pattern)
            .map(|like| format!("line like '{like}'")),
        LineFilterOp::NotPattern => pattern_line_filter_like_pattern(&filter.pattern)
            .map(|like| format!("line not like '{like}'")),
    }
}
