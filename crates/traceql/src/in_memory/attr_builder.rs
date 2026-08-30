use super::*;

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
        match (self, value) {
            (Self::Str(b), Some(AttrValue::Str(v))) => b.append_value(v),
            (Self::Str(b), _) => b.append_null(),
            (Self::Int(b), Some(AttrValue::Int(v))) => b.append_value(*v),
            (Self::Int(b), _) => b.append_null(),
            (Self::Float(b), Some(AttrValue::Float(v))) => b.append_value(*v),
            (Self::Float(b), _) => b.append_null(),
            (Self::Bool(b), Some(AttrValue::Bool(v))) => b.append_value(*v),
            (Self::Bool(b), _) => b.append_null(),
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
