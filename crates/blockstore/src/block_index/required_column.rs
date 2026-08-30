use super::*;

/// One required column in a signal block schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequiredColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl RequiredColumn {
    #[must_use]
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }
}
