use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DetectedFieldType {
    Boolean,
    Int,
    Float,
    Duration,
    Bytes,
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

    pub(crate) fn as_loki_str(self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Int => "int",
            Self::Float => "float",
            Self::Duration => "duration",
            Self::Bytes => "bytes",
            Self::String => "string",
        }
    }
}
