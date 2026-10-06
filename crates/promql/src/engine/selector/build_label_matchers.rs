use super::{LabelMatcher, MatchOp, prom_label};

pub(crate) fn build_label_matchers(
    metric_name: Option<&str>,
    matchers: &[prom_label::Matcher],
) -> Vec<LabelMatcher> {
    let mut out = Vec::new();
    if let Some(name) = metric_name {
        out.push(LabelMatcher::new("__name__", MatchOp::Eq, name));
    }
    for matcher in matchers {
        let op = match matcher.op {
            prom_label::MatchOp::Equal => MatchOp::Eq,
            prom_label::MatchOp::NotEqual => MatchOp::Neq,
            prom_label::MatchOp::Re(_) => MatchOp::Re,
            prom_label::MatchOp::NotRe(_) => MatchOp::Nre,
        };
        let next = LabelMatcher::new(&matcher.name, op, &matcher.value);
        if !out.iter().any(|existing| {
            existing.name == next.name && existing.op == next.op && existing.value == next.value
        }) {
            out.push(next);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retains_opposing_matchers_with_the_same_name_and_value() {
        let promql_parser::parser::Expr::VectorSelector(selector) =
            promql_parser::parser::parse(r#"{__name__=~".+_info",__name__!~".+_info"}"#).unwrap()
        else {
            panic!("selector expected")
        };
        let matchers = build_label_matchers(None, &selector.matchers.matchers);
        assert2::assert!(matchers.len() == 2);
        assert2::assert!(matchers[0].op != matchers[1].op);
    }
}
