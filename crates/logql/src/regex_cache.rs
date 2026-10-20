use std::borrow::Cow;

use regex::Regex;

#[derive(Clone, Debug)]
pub(crate) struct RegexCache {
    source: String,
    regex: Regex,
}

impl RegexCache {
    pub(crate) fn new(source: String, regex: Regex) -> Self {
        Self { source, regex }
    }
}

pub(crate) fn resolve_regex<'a>(
    cached: Option<&'a RegexCache>,
    source: &str,
    normalize: impl FnOnce(&str) -> String,
    expectation: &str,
) -> Cow<'a, Regex> {
    if let Some(cached) = cached
        && cached.source == source
    {
        return Cow::Borrowed(&cached.regex);
    }
    Cow::Owned(Regex::new(&normalize(source)).expect(expectation))
}
