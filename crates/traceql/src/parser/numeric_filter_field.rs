use super::{Field, Scope};

pub(crate) fn numeric_filter_field() -> Field {
    Field {
        scope: Scope::Both,
        key: String::new(),
    }
}
