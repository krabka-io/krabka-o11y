use super::*;

pub(crate) fn format_label_matcher(matcher: &krabka_logql::LabelMatcher) -> String {
    format!(
        "{}{}{}",
        matcher.name,
        match matcher.op {
            MatchOp::Equal => "=",
            MatchOp::NotEqual => "!=",
            MatchOp::RegexEqual => "=~",
            MatchOp::RegexNotEqual => "!~",
        },
        quote_logql_string(&matcher.value)
    )
}
