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
        if self == Self::String || other == Self::String {
            Self::String
        } else if self == Self::Bytes && other == Self::Bytes {
            Self::Bytes
        } else if self == Self::Duration && other == Self::Duration {
            Self::Duration
        } else if self == Self::Float || other == Self::Float {
            Self::Float
        } else if self == Self::Int && other == Self::Int {
            Self::Int
        } else if self == Self::Boolean && other == Self::Boolean {
            Self::Boolean
        } else {
            Self::String
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
