use std::collections::BTreeSet;

use super::{ScopedTag, TagScope};

/// Tag names grouped by source scope before the requested scope is applied.
#[derive(Default)]
pub struct TagNameGroups {
    pub resource: BTreeSet<String>,
    pub span: BTreeSet<String>,
    pub event: BTreeSet<String>,
    pub link: BTreeSet<String>,
    pub instrumentation: BTreeSet<String>,
}

impl TagNameGroups {
    /// Returns nonempty groups allowed by `scope`, with intrinsic names
    /// included when the store has matching spans.
    pub fn into_scoped_tags(
        self,
        scope: Option<TagScope>,
        include_intrinsic: bool,
        intrinsic_tags: &[&str],
    ) -> Vec<ScopedTag> {
        let mut out = Vec::new();
        Self::push_group(&mut out, scope, TagScope::Resource, self.resource);
        Self::push_group(&mut out, scope, TagScope::Span, self.span);
        if include_intrinsic && scope.is_none_or(|scope| scope == TagScope::Intrinsic) {
            out.push(ScopedTag {
                scope: TagScope::Intrinsic,
                tags: intrinsic_tags
                    .iter()
                    .map(|tag| (*tag).to_string())
                    .collect(),
            });
        }
        Self::push_group(&mut out, scope, TagScope::Event, self.event);
        Self::push_group(&mut out, scope, TagScope::Link, self.link);
        Self::push_group(
            &mut out,
            scope,
            TagScope::Instrumentation,
            self.instrumentation,
        );
        out
    }

    fn push_group(
        out: &mut Vec<ScopedTag>,
        requested: Option<TagScope>,
        scope: TagScope,
        tags: BTreeSet<String>,
    ) {
        if requested.is_none_or(|requested| requested == scope) && !tags.is_empty() {
            out.push(ScopedTag {
                scope,
                tags: tags.into_iter().collect(),
            });
        }
    }
}
