use super::Span;

pub(crate) fn root_span<'a>(spans: &'a [&'a Span]) -> Option<&'a Span> {
    spans
        .iter()
        .copied()
        .find(|span| span.is_root())
        .or_else(|| spans.iter().copied().min_by_key(|span| span.start_ns))
}
