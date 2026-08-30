use super::*;

pub(crate) fn build_label_matchers(
    metric_name: Option<&str>,
    matchers: &[prom_label::Matcher],
) -> Vec<LabelMatcher> {
    let mut out = Vec::new();
    if let Some(name) = metric_name {
        out.push(LabelMatcher::new("__name__", MatchOp::Eq, name));
    }
    let mut seen = out
        .iter()
        .map(|matcher| (matcher.name.clone(), matcher.value.clone()))
        .collect::<BTreeSet<_>>();
    for matcher in matchers {
        let op = match matcher.op {
            prom_label::MatchOp::Equal => MatchOp::Eq,
            prom_label::MatchOp::NotEqual => MatchOp::Neq,
            prom_label::MatchOp::Re(_) => MatchOp::Re,
            prom_label::MatchOp::NotRe(_) => MatchOp::Nre,
        };
        let next = LabelMatcher::new(&matcher.name, op, &matcher.value);
        if seen.insert((next.name.clone(), next.value.clone())) {
            out.push(next);
        }
    }
    out
}
