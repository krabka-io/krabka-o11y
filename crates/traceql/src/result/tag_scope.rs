/// Tag discovery scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TagScope {
    Resource,
    Span,
    Intrinsic,
    Event,
    Link,
    Instrumentation,
}

impl TagScope {
    /// Splits a `resource.` or `span.` scope prefix off an attribute tag.
    ///
    /// Returns the bare attribute name, with the scope the prefix named, or
    /// the tag unchanged and no scope when it has neither prefix.
    #[must_use]
    pub fn split_resource_or_span_prefix(tag: &str) -> (&str, Option<Self>) {
        if let Some(tag) = tag.strip_prefix("resource.") {
            (tag, Some(Self::Resource))
        } else if let Some(tag) = tag.strip_prefix("span.") {
            (tag, Some(Self::Span))
        } else {
            (tag, None)
        }
    }
}
