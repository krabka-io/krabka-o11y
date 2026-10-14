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

/// A normalized query tag and the attribute prefix it names, when present.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParsedTagScope<'a> {
    pub tag: &'a str,
    pub attribute: TagScopePrefix<'a>,
}

impl TagScope {
    /// Removes an optional leading dot and resolves instrumentation,
    /// resource, or span attribute prefixes.
    #[must_use]
    pub fn parse_attribute_tag(tag: &str) -> ParsedTagScope<'_> {
        let tag = tag.strip_prefix('.').unwrap_or(tag);
        let attribute = match tag.strip_prefix("instrumentation.") {
            Some(tag) => TagScopePrefix {
                tag,
                scope: Some(Self::Instrumentation),
            },
            None => Self::split_resource_or_span_prefix(tag),
        };
        ParsedTagScope { tag, attribute }
    }

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
