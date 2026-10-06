//! Derive constructors and ordered Arrow arrays from column builder fields.

use moxy::{
    ast::{Attributed, Declaration, Expr, ItemFn, ParseError},
    diagnostic::SpanExt,
    token::{Spanner, TokenStream},
};

#[derive(Default, moxy::FromMeta)]
struct Columns {
    #[meta(default)]
    args: String,
    #[meta(default)]
    named: bool,
}

#[derive(Default, moxy::FromMeta)]
struct Column {
    #[meta(default)]
    init: Option<String>,
    #[meta(default)]
    name: Option<String>,
}

/// Derive `new` and `finish` for a struct of Arrow array builders.
///
/// Fields finish in declaration order. Builders use `Default` unless a
/// `#[column(init = "expression")]` attribute supplies their initializer.
/// `#[columns(args = "capacity: usize")]` adds constructor arguments.
/// `#[columns(named)]` returns named arrays and requires a
/// `#[column(name = "COLUMN_CONSTANT")]` attribute on every field.
#[moxy::derive(ColumnBuilders, attributes(columns, column))]
pub fn column_builders(declaration: Declaration) -> Result<TokenStream, ParseError> {
    expand(declaration)
}

fn expand(declaration: Declaration) -> Result<TokenStream, ParseError> {
    let Declaration::Struct(item) = declaration else {
        return declaration
            .span()
            .error("ColumnBuilders requires a named struct")
            .into();
    };
    let Some(named) = item.fields.as_named() else {
        return item
            .ident
            .span()
            .error("ColumnBuilders requires named fields")
            .into();
    };
    if item.generics.lt.is_some() || item.generics.where_clause.is_some() {
        return item
            .ident
            .span()
            .error("ColumnBuilders does not support generic structs")
            .into();
    }
    let options = item.parse_meta::<Columns>("columns")?.unwrap_or_default();
    let constructor_source = format!("fn new({}) {{}}", options.args);
    let constructor: ItemFn = moxy::parse!(constructor_source)?;
    let params = constructor.sig.params;
    let fields = named
        .fields
        .iter()
        .map(|field| {
            let options = field.parse_meta::<Column>("column")?.unwrap_or_default();
            let initializer = options
                .init
                .map(|source| moxy::parse!(source as Expr))
                .transpose()?;
            let name = options
                .name
                .map(|source| moxy::parse!(source as Expr))
                .transpose()?;
            Ok((field, initializer, name))
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    for (field, _, name) in &fields {
        if options.named && name.is_none() {
            return field
                .span()
                .error("named columns require #[column(name = \"COLUMN_CONSTANT\")]")
                .into();
        }
        if !options.named && name.is_some() {
            return field
                .span()
                .error("column names require #[columns(named)]")
                .into();
        }
    }
    Ok(moxy::template! {
        impl {{ &item.ident }} {
            {{ &item.vis }} fn new {{ params }} -> Self {
                Self {
                    @for (field, initializer, _) in &fields {
                        {{ &field.ident }}:
                        @if let Some(initializer) = initializer {
                            {{ initializer }}
                        } @else {
                            <{{ &field.ty }} as ::core::default::Default>::default()
                        },
                    }
                }
            }

            {{ &item.vis }} fn finish(mut self) ->
            @if options.named {
                ::std::vec::Vec<(&'static str, ::arrow::array::ArrayRef)>
            } @else {
                ::std::vec::Vec<::arrow::array::ArrayRef>
            }
            {
                ::std::vec![
                    @for (field, _, name) in &fields {
                        @if let Some(name) = name {
                            ({{ name }}, ::std::sync::Arc::new(self.{{ &field.ident }}.finish())),
                        } @else {
                            ::std::sync::Arc::new(self.{{ &field.ident }}.finish()),
                        }
                    }
                ]
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unsupported_structs_and_invalid_column_options() {
        for source in [
            "enum Columns { Empty }",
            "struct Columns(Int64Builder);",
            "struct Columns<T> { value: T }",
            "#[columns(named)] struct Columns { value: Int64Builder }",
            "struct Columns { #[column(name = \"NAME\")] value: Int64Builder }",
            "struct Columns { #[column(init = \"let\")] value: Int64Builder }",
            "#[columns(args = \"(\")] struct Columns { value: Int64Builder }",
        ] {
            let declaration: Declaration = moxy::parse!(source).unwrap();
            assert2::assert!(expand(declaration).is_err(), "{source}");
        }
    }
}
