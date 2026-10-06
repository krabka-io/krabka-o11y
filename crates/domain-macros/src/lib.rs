//! Derive domain names and delegate object-store methods.
//!
//! [`EnumName`] keeps each variant's spelling next to its declaration. It
//! generates a constant accessor and a display implementation from that name.
//! [`delegate_object_store`] adds required forwarding methods to store wrappers.

use std::collections::HashSet;

use moxy::{
    ast::{Attributed, Declaration, ParseError},
    diagnostic::SpanExt,
    token::{Spanner, TokenStream},
};

#[derive(moxy::FromMeta)]
struct Name {
    value: String,
}

#[derive(Default, moxy::FromMeta)]
struct EnumNameOptions {
    #[meta(default)]
    clap: bool,
    #[meta(default)]
    parse: bool,
}

#[derive(moxy::FromMeta)]
struct ClapName {
    name: String,
    #[meta(default, rename = "hide")]
    _hide: bool,
}

/// Derive `as_str(self)` and `Display` from explicit enum variant names.
///
/// Every variant needs `#[name(value = "spelling")]`. The enum must have
/// at least one unit variant and no generic parameters. The generated `as_str` is a
/// `const fn` and has the same visibility as the enum. Neither method changes
/// case or punctuation. Other derives control serialization.
/// `#[enum_name(parse)]` also generates `from_name(&str)`, which returns `None`
/// for unknown names. It compares the full name with exact case.
/// Names must be unique when parsing is enabled.
/// `#[enum_name(clap)]` reads explicit `#[value(name = "spelling")]` names
/// instead. Hidden clap variants keep their names; skipped variants are rejected.
/// Conditional variants are rejected.
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
#[moxy::derive(EnumName, attributes(name, enum_name))]
pub fn enum_name(declaration: Declaration) -> Result<TokenStream, ParseError> {
    expand(declaration)
}

fn expand(declaration: Declaration) -> Result<TokenStream, ParseError> {
    let Declaration::Enum(item) = declaration else {
        return declaration.span().error("EnumName requires an enum").into();
    };
    if item
        .attrs()
        .iter()
        .filter(|attribute| attribute.path.is_ident("enum_name"))
        .count()
        > 1
    {
        return item
            .ident
            .span()
            .error("EnumName requires at most one configuration attribute")
            .into();
    }
    let options = item
        .parse_meta::<EnumNameOptions>("enum_name")?
        .unwrap_or_default();
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
            if variant.attrs().iter().any(|attribute| {
                attribute.path.is_ident("cfg") || attribute.path.is_ident("cfg_attr")
            }) {
                return Err(variant
                    .ident
                    .span()
                    .error("EnumName does not support conditional variants")
                    .into());
            }
            let source = if options.clap { "value" } else { "name" };
            if variant
                .attrs()
                .iter()
                .filter(|attribute| attribute.path.is_ident(source))
                .count()
                != 1
                || (options.clap
                    && variant
                        .attrs()
                        .iter()
                        .any(|attribute| attribute.path.is_ident("name")))
            {
                return Err(variant
                    .ident
                    .span()
                    .error("EnumName requires exactly one explicit name from its selected source")
                    .into());
            }
            let name = if options.clap {
                let value = variant.parse_meta::<ClapName>("value")?.ok_or_else(|| {
                    variant
                        .ident
                        .span()
                        .error("EnumName requires an explicit clap name")
                })?;
                value.name
            } else {
                variant
                    .parse_meta::<Name>("name")?
                    .ok_or_else(|| {
                        variant
                            .ident
                            .span()
                            .error("EnumName requires a variant name")
                    })?
                    .value
            };
            Ok((&variant.ident, name))
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    if options.parse {
        let mut names = HashSet::new();
        for (variant, name) in &variants {
            if !names.insert(name) {
                return variant
                    .span()
                    .error("EnumName parsing requires unique variant names")
                    .into();
            }
        }
    }
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
            @if options.parse {
                /// The variant with this exact name, or `None` for an unknown name.
                #[must_use]
                {{ &item.vis }} fn from_name(name: &str) -> ::core::option::Option<Self> {
                    match name {
                        @for (variant, name) in &variants {
                            {{ name }} => ::core::option::Option::Some(Self::{{ variant }}),
                        }
                        _ => ::core::option::Option::None,
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
            "#[enum_name(clap)] enum MissingClap { #[value] A }",
            "#[enum_name(clap)] enum MissingClapName { #[value(hide = true)] A }",
            "#[enum_name(clap)] enum Skipped { #[value(name = \"a\", skip)] A }",
            "#[enum_name(clap)] enum SkippedFalse { #[value(name = \"a\", skip = false)] A }",
            "#[enum_name(clap)] enum SkippedTrue { #[value(name = \"a\", skip = true)] A }",
            "#[enum_name(clap)] enum Conflict { #[name(value = \"a\")] #[value(name = \"b\")] A }",
            "#[enum_name(clap)] enum DuplicateClap { #[value(name = \"a\")] #[value(name = \"b\")] A }",
            "#[enum_name(clap)] #[enum_name(clap)] enum DuplicateOptions { #[value(name = \"a\")] A }",
            "enum Conditional { #[cfg(unix)] #[name(value = \"a\")] A }",
            "enum ConditionalAttr { #[cfg_attr(unix, name(value = \"a\"))] A }",
            "#[enum_name(parse)] enum Ambiguous { #[name(value = \"a\")] A, #[name(value = \"a\")] B }",
            "#[enum_name(clap, parse)] enum AmbiguousClap { #[value(name = \"a\")] A, #[value(name = \"a\")] B }",
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

mod delegate_object_store;

/// Add missing required `ObjectStore` methods that forward to an inner store.
///
/// The argument is the delegate expression, such as `self.inner`. Put this
/// attribute before `#[async_trait::async_trait]`. Explicit overrides and optional
/// trait defaults keep their behavior. Consumers need `object_store`, `futures`,
/// and `async_trait` dependencies.
#[moxy::attribute(name = "delegate_object_store")]
pub fn delegate_object_store(
    meta: TokenStream,
    tokens: TokenStream,
) -> Result<TokenStream, ParseError> {
    self::delegate_object_store::expand(meta, tokens)
}
