pub(crate) fn merge_scope_spans(
    existing: &mut crate::frontend::wire::ResourceSpansJson,
    incoming: Vec<crate::frontend::wire::ScopeSpansJson>,
) {
    for ss in incoming {
        if let Some(group) = existing
            .scope_spans
            .iter_mut()
            .find(|e| e.scope == ss.scope)
        {
            group.spans.extend(ss.spans);
        } else {
            existing.scope_spans.push(ss);
        }
    }
}
