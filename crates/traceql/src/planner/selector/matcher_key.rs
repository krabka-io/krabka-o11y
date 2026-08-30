use super::*;

pub(crate) fn matcher_key(field: &Field) -> String {
    match &field.scope {
        Scope::Intrinsic(intrinsic) => intrinsic_match_key(intrinsic).to_string(),
        _ => field.key.clone(),
    }
}
