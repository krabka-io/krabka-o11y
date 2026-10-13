use super::{
    Arc, ArrayRef, AttrValue, BooleanBuilder, DataType, Float64Builder, Int64Builder, StringBuilder,
};

pub(crate) enum AttrBuilder {
    Str(StringBuilder),
    Int(Int64Builder),
    Float(Float64Builder),
    Bool(BooleanBuilder),
}

impl AttrBuilder {
    pub(crate) fn new(dt: &DataType) -> Self {
        match dt {
            DataType::Utf8 => Self::Str(StringBuilder::new()),
            DataType::Int64 => Self::Int(Int64Builder::new()),
            DataType::Float64 => Self::Float(Float64Builder::new()),
            DataType::Boolean => Self::Bool(BooleanBuilder::new()),
            other => panic!("unsupported attribute data type {other:?}"),
        }
    }

    pub(crate) fn append(&mut self, value: Option<&AttrValue>) {
        match self {
            Self::Str(builder) => match value {
                Some(AttrValue::Str(value)) => builder.append_value(value),
                _ => builder.append_null(),
            },
            Self::Int(builder) => match value {
                Some(AttrValue::Int(value)) => builder.append_value(*value),
                _ => builder.append_null(),
            },
            Self::Float(builder) => match value {
                Some(AttrValue::Float(value)) => builder.append_value(*value),
                _ => builder.append_null(),
            },
            Self::Bool(builder) => match value {
                Some(AttrValue::Bool(value)) => builder.append_value(*value),
                _ => builder.append_null(),
            },
        }
    }

    pub(crate) fn finish(self) -> ArrayRef {
        match self {
            Self::Str(mut b) => Arc::new(b.finish()),
            Self::Int(mut b) => Arc::new(b.finish()),
            Self::Float(mut b) => Arc::new(b.finish()),
            Self::Bool(mut b) => Arc::new(b.finish()),
        }
    }
}
