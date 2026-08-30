use super::*;

pub(crate) fn inject_shard_into_selector(selector: &mut VectorSelector, shard: QueryShard) {
    if selector
        .matchers
        .matchers
        .iter()
        .any(|matcher| matcher.name == QUERY_SHARD_LABEL)
    {
        return;
    }

    selector.matchers.matchers.push(prom_label::Matcher::new(
        prom_label::MatchOp::Equal,
        QUERY_SHARD_LABEL,
        &shard.selector_value(),
    ));
}
