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

/// A tag after removing its optional resource or span prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TagScopePrefix<'a> {
    pub tag: &'a str,
    pub scope: Option<TagScope>,
}

impl TagScope {
    /// Splits a `resource.` or `span.` scope prefix off an attribute tag.
    ///
    /// Returns the bare attribute name, with the scope the prefix named, or
    /// the tag unchanged and no scope when it has neither prefix.
    #[must_use]
    pub fn split_resource_or_span_prefix(tag: &str) -> TagScopePrefix<'_> {
        if let Some(tag) = tag.strip_prefix("resource.") {
            TagScopePrefix {
                tag,
                scope: Some(Self::Resource),
            }
        } else if let Some(tag) = tag.strip_prefix("span.") {
            TagScopePrefix {
                tag,
                scope: Some(Self::Span),
            }
        } else {
            TagScopePrefix { tag, scope: None }
        }
    }
}
