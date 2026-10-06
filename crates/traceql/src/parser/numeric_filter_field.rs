use super::{Field, Intrinsic, Scope};

pub(crate) fn numeric_filter_field() -> Field {
    Field {
        scope: Scope::Intrinsic(Intrinsic::Duration),
        key: String::new(),
    }
}
