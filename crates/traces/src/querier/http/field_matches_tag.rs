use super::*;

pub(crate) fn field_matches_tag(field: &Field, tag: &str) -> bool {
    let tag = tag.strip_prefix('.').unwrap_or(tag);
    match &field.scope {
        Scope::Both => tag == field.key.as_str(),
        Scope::Span => tag == format!("span.{}", field.key),
        Scope::Resource => tag == format!("resource.{}", field.key),
        Scope::Parent => tag == format!("parent.{}", field.key),
        Scope::Event => tag == format!("event.{}", field.key),
        Scope::Link => tag == format!("link.{}", field.key),
        Scope::Instrumentation => tag == format!("instrumentation.{}", field.key),
        Scope::Intrinsic(intrinsic) => tag == intrinsic_tag_name(intrinsic),
    }
}
