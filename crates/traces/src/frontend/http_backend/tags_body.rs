use super::{ScopeTagsJson, parse_scope};

/// The `/api/v2/search/tags` body: `{ scopes: [{ name, tags }], metrics }`.
#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct TagsBody {
    #[serde(default)]
    pub(crate) scopes: Vec<ScopeTagsJson>,
    #[serde(default)]
    pub(crate) metrics: crate::frontend::wire::Metrics,
}

impl TagsBody {
    pub(crate) fn scoped_tags(&self) -> Vec<krabka_traceql::ScopedTag> {
        self.scopes
            .iter()
            .map(|s| krabka_traceql::ScopedTag {
                scope: parse_scope(&s.name),
                tags: s.tags.clone(),
            })
            .collect()
    }
}
