use super::{Result, ScopedTag, TagScope, TypedValue};

/// The tag names and tag values a span source holds for a tenant.
///
/// Both [`super::SpanStore`] and live (unflushed) span sources answer Tempo's
/// tag-autocomplete API, so they share this supertrait.
#[async_trait::async_trait]
pub trait TagCatalog: Send + Sync {
    /// The distinct attribute and intrinsic tag names in `[start_ns, end_ns)`,
    /// limited to `scope` when one is given.
    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>>;

    /// The distinct typed values `tag` takes in `[start_ns, end_ns)`.
    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>>;
}
