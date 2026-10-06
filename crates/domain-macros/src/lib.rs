//! Derive explicit names for domain enums.
//!
//! [`EnumName`] keeps each variant's spelling next to its declaration. It
//! generates a constant accessor and a display implementation from that name.

use moxy::{
    ast::{Attributed, Declaration, ParseError},
    diagnostic::SpanExt,
    token::{Spanner, TokenStream},
};

#[derive(moxy::FromMeta)]
struct Name {
    value: String,
}

/// Derive `as_str(self)` and `Display` from explicit enum variant names.
///
/// Every variant needs `#[name(value = "spelling")]`. The enum must have
/// at least one unit variant and no generic parameters. The generated `as_str` is a
/// `const fn` and has the same visibility as the enum. Neither method changes
/// case or punctuation. Other derives control serialization and parsing.
///
/// # Examples
///
/// ```
/// use krabka_domain_macros::EnumName;
///
/// #[derive(Clone, Copy, EnumName)]
/// enum Role {
///     #[name(value = "block-builder")]
///     BlockBuilder,
/// }
///
/// const NAME: &str = Role::BlockBuilder.as_str();
/// assert2::assert!(NAME == "block-builder");
/// assert2::assert!(Role::BlockBuilder.to_string() == NAME);
/// ```
#[moxy::derive(EnumName, attributes(name))]
pub fn enum_name(declaration: Declaration) -> Result<TokenStream, ParseError> {
    expand(declaration)
}

fn expand(declaration: Declaration) -> Result<TokenStream, ParseError> {
    let Declaration::Enum(item) = declaration else {
        return declaration.span().error("EnumName requires an enum").into();
    };
    if item.generics.lt.is_some() || item.generics.where_clause.is_some() {
        return item
            .ident
            .span()
            .error("EnumName does not support generic enums")
            .into();
    }
    if item.variants.is_empty() {
        return item
            .ident
            .span()
            .error("EnumName requires at least one variant")
            .into();
    }
    let variants = item
        .variants
        .iter()
        .map(|variant| {
            if !variant.fields.is_unit() {
                return Err(variant
                    .ident
                    .span()
                    .error("EnumName requires unit variants")
                    .into());
            }
            let names = variant
                .attrs()
                .iter()
                .filter(|attribute| attribute.path.is_ident("name"))
                .count();
            if names != 1 {
                return Err(variant
                    .ident
                    .span()
                    .error(
                        "EnumName requires exactly one #[name(value = \"spelling\")] per variant",
                    )
                    .into());
            }
            let name = variant.parse_meta::<Name>("name")?.ok_or_else(|| {
                variant
                    .ident
                    .span()
                    .error("EnumName requires a variant name")
            })?;
            Ok((&variant.ident, name.value))
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    Ok(moxy::template! {
        impl {{ &item.ident }} {
            /// The explicit name of this variant.
            #[must_use]
            {{ &item.vis }} const fn as_str(self) -> &'static str {
                match self {
                    @for (variant, name) in &variants {
                        Self::{{ variant }} => {{ name }},
                    }
                }
            }
        }

        impl ::core::fmt::Display for {{ &item.ident }} {
            fn fmt(&self, formatter: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                formatter.write_str(match self {
                    @for (variant, name) in &variants {
                        Self::{{ variant }} => {{ name }},
                    }
                })
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unsupported_declarations_and_invalid_names() {
        for source in [
            "struct NotAnEnum;",
            "enum Empty {}",
            "enum Generic<T> { #[name(value = \"a\")] A(T) }",
            "enum Tuple { #[name(value = \"a\")] A(u8) }",
            "enum Named { #[name(value = \"a\")] A { value: u8 } }",
            "enum Missing { A }",
            "enum MissingValue { #[name] A }",
            "enum WrongType { #[name(value = 1)] A }",
            "enum Duplicate { #[name(value = \"a\")] #[name(value = \"b\")] A }",
        ] {
            let declaration: Declaration = moxy::parse!(source).unwrap();
            assert2::assert!(expand(declaration).is_err(), "{source}");
        }
    }
}
