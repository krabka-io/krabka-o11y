use krabka_domain_macros::EnumName;

#[derive(Clone, Copy, Debug, Eq, PartialEq, EnumName)]
#[enum_name(accessor = "as_loki_str")]
pub(crate) enum DetectedFieldType {
    #[name(value = "boolean")]
    Boolean,
    #[name(value = "int")]
    Int,
    #[name(value = "float")]
    Float,
    #[name(value = "duration")]
    Duration,
    #[name(value = "bytes")]
    Bytes,
    #[name(value = "string")]
    String,
}

impl DetectedFieldType {
    pub(crate) fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::String, _) | (_, Self::String) => Self::String,
            (Self::Bytes, Self::Bytes) => Self::Bytes,
            (Self::Duration, Self::Duration) => Self::Duration,
            (Self::Float, _) | (_, Self::Float) => Self::Float,
            (Self::Int, Self::Int) => Self::Int,
            (Self::Boolean, Self::Boolean) => Self::Boolean,
            _ => Self::String,
        }
    }
}
