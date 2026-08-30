use super::{
    Arc, ArrayRef, AttrValue, BooleanBuilder, Float64Builder, Int32Type, Int64Builder,
    PromotedSpanAttr, PromotedSpanAttrType, SpanAttr, StringDictionaryBuilder, promoted_attr_value,
};

pub(crate) enum PromotedAttrBuilder {
    String {
        key: String,
        builder: StringDictionaryBuilder<Int32Type>,
    },
    Int {
        key: String,
        builder: Int64Builder,
    },
    Double {
        key: String,
        builder: Float64Builder,
    },
    Bool {
        key: String,
        builder: BooleanBuilder,
    },
}

impl PromotedAttrBuilder {
    pub(crate) fn new(attr: &PromotedSpanAttr) -> Self {
        match attr.value_type {
            PromotedSpanAttrType::String => Self::String {
                key: attr.key.clone(),
                builder: StringDictionaryBuilder::new(),
            },
            PromotedSpanAttrType::Int => Self::Int {
                key: attr.key.clone(),
                builder: Int64Builder::new(),
            },
            PromotedSpanAttrType::Double => Self::Double {
                key: attr.key.clone(),
                builder: Float64Builder::new(),
            },
            PromotedSpanAttrType::Bool => Self::Bool {
                key: attr.key.clone(),
                builder: BooleanBuilder::new(),
            },
        }
    }

    pub(crate) fn append(&mut self, attrs: &[SpanAttr]) {
        match self {
            Self::String { key, builder } => match promoted_attr_value(attrs, key) {
                Some(AttrValue::Str(values)) => builder.append_option(values.first()),
                _ => builder.append_null(),
            },
            Self::Int { key, builder } => match promoted_attr_value(attrs, key) {
                Some(AttrValue::Int(values)) => builder.append_option(values.first().copied()),
                _ => builder.append_null(),
            },
            Self::Double { key, builder } => match promoted_attr_value(attrs, key) {
                Some(AttrValue::Double(values)) => builder.append_option(values.first().copied()),
                _ => builder.append_null(),
            },
            Self::Bool { key, builder } => match promoted_attr_value(attrs, key) {
                Some(AttrValue::Bool(values)) => builder.append_option(values.first().copied()),
                _ => builder.append_null(),
            },
        }
    }

    pub(crate) fn finish(self) -> ArrayRef {
        match self {
            Self::String { mut builder, .. } => Arc::new(builder.finish()),
            Self::Int { mut builder, .. } => Arc::new(builder.finish()),
            Self::Double { mut builder, .. } => Arc::new(builder.finish()),
            Self::Bool { mut builder, .. } => Arc::new(builder.finish()),
        }
    }
}
