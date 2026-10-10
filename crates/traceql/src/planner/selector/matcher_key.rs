use super::{Field, Scope};

pub(crate) fn matcher_key(field: &Field) -> String {
    match &field.scope {
        Scope::Intrinsic(intrinsic) => intrinsic.tag_name().to_string(),
        _ => field.key.clone(),
    }
}
