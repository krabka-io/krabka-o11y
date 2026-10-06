//! Syntax-aware transformations for generated Rust protocol types.

use std::{error::Error, ops::Range};

use moxy::{
    ast::{Attribute, ItemImpl, ItemStruct},
    token::{Span, ToTokenStream, TokenStream, TokenTree, source::SourceMap},
};

type CodegenResult<T> = Result<T, Box<dyn Error>>;

/// Which generated public methods receive a `must_use` attribute.
#[derive(Clone, Copy)]
pub enum MustUse {
    /// Enum name conversion methods only.
    EnumNames,
    /// All public synchronous methods.
    PublicMethods,
}

/// Adds `must_use` to selected public methods and named builder structs.
///
/// # Errors
/// Returns an error if the generated Rust cannot be parsed or formatted.
pub fn annotate_must_use(
    source: &str,
    policy: MustUse,
    builders: &[&str],
) -> CodegenResult<String> {
    let tokens: TokenStream = source.parse()?;
    let attribute_tokens = moxy::template! { #[must_use] };
    let attribute: Attribute = moxy::parse!(attribute_tokens)?;
    let rendered = format!("{}\n", moxy::fmt!(&attribute)?);
    let mut positions = Vec::new();
    collect_annotations(&tokens, policy, builders, &mut positions);
    let mut output = source.to_owned();
    positions.sort_unstable();
    for position in positions.into_iter().rev() {
        output.insert_str(position, &rendered);
    }
    Ok(output)
}

fn collect_annotations(
    tokens: &TokenStream,
    policy: MustUse,
    builders: &[&str],
    positions: &mut Vec<usize>,
) {
    for (index, token) in tokens.iter().enumerate() {
        if let TokenTree::Group(group) = token {
            // Macro inputs are opaque Rust tokens, not declarations.
            if index == 0 || tokens[index - 1].to_string() != "!" {
                collect_annotations(&group.tokens, policy, builders, positions);
            }
        }
        if token.text() != Some("pub") {
            continue;
        }
        let Some(kind) = tokens.get(index + 1).and_then(TokenTree::text) else {
            continue;
        };
        let Some(name) = tokens.get(index + 2).and_then(TokenTree::text) else {
            continue;
        };
        let selected = match kind {
            "fn" => match policy {
                MustUse::EnumNames => matches!(name, "as_str_name" | "from_str_name"),
                MustUse::PublicMethods => true,
            },
            "struct" => builders.contains(&name),
            _ => false,
        };
        if !selected {
            continue;
        }
        let mut start = index;
        let mut already_marked = false;
        while start >= 2 && tokens[start - 2].to_string() == "#" {
            let Some(group) = tokens[start - 1].as_group() else {
                break;
            };
            already_marked |= group.tokens.first().and_then(TokenTree::text) == Some("must_use");
            start -= 2;
        }
        if !already_marked {
            positions.push(source_range(tokens[start].span()).start);
        }
    }
}

fn source_range(span: Span) -> Range<usize> {
    // Resolve global lexer offsets against their source before editing. The
    // lexer reports UTF-8 byte offsets, including text after non-ASCII literals.
    match span {
        Span::Fallback(span) => SourceMap::with(|map| {
            let source = map.find(span).expect("parsed span has a source");
            source.location(span.byte_range().start).index()
                ..source.location(span.byte_range().end).index()
        }),
        Span::Compiler(_) => unreachable!("build scripts parse source outside procedural macros"),
    }
}

/// Compacts a generated trait implementation without changing literal text.
///
/// # Errors
/// Returns an error if parsing fails or the selected implementation is absent
/// or appears more than once.
pub fn compact_impl(source: &str, trait_name: &str, type_name: &str) -> CodegenResult<String> {
    let tokens: TokenStream = source.parse()?;
    let mut selected = Vec::new();
    for (start, token) in tokens.iter().enumerate() {
        if token.text() != Some("impl") {
            continue;
        }
        let Some(end) = tokens[start..]
            .iter()
            .position(|token| token.delim() == Some(moxy::token::Delim::Brace))
            .map(|offset| start + offset)
        else {
            continue;
        };
        // Parse only the header: generated method bodies can contain syntax
        // that a Rust AST parser does not yet support.
        let mut header: TokenStream = tokens[start..end].iter().cloned().collect();
        let empty = moxy::template! { {} };
        header.extend(empty);
        let implementation: ItemImpl = moxy::parse!(header)?;
        let Some(trait_ref) = &implementation.trait_ref else {
            continue;
        };
        let Some(trait_ident) = trait_ref.path.last() else {
            continue;
        };
        let moxy::ast::Type::Path(self_ty) = &implementation.self_ty else {
            continue;
        };
        let Some(type_ident) = self_ty.path.last() else {
            continue;
        };
        if trait_ident.ident.text() == trait_name && type_ident.ident.text() == type_name {
            selected.push((start, end));
        }
    }
    let [(start, end)] = selected.as_slice() else {
        return Err("generated trait implementation is missing or ambiguous".into());
    };
    let implementation: TokenStream = tokens[*start..=*end].iter().cloned().collect();
    let range = source_range(implementation.span());
    let mut output = source.to_owned();
    output.replace_range(range, &implementation.to_string());
    Ok(output)
}

/// Replaces one generated struct after checking its fields and prost metadata.
///
/// Derive ordering and source formatting do not affect the schema check.
///
/// # Errors
/// Returns an error if parsing fails, the struct is absent or ambiguous, or its
/// field schema differs from the expected definition.
pub fn replace_struct(source: &str, expected: &str, replacement: &str) -> CodegenResult<String> {
    let tokens: TokenStream = source.parse()?;
    let expected: ItemStruct = moxy::parse!(expected)?;
    let _: TokenStream = replacement.parse()?;
    let mut selected = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        if token.text() != Some("struct")
            || tokens.get(index + 1).and_then(TokenTree::text) != Some(expected.ident.text())
        {
            continue;
        }
        let Some(end) = tokens[index..]
            .iter()
            .position(|token| token.delim() == Some(moxy::token::Delim::Brace))
            .map(|offset| index + offset)
        else {
            continue;
        };
        let mut start = index;
        if start > 0 && tokens[start - 1].text() == Some("pub") {
            start -= 1;
        }
        while start >= 2
            && tokens[start - 2].to_string() == "#"
            && tokens[start - 1].as_group().is_some()
        {
            start -= 2;
        }
        let definition: TokenStream = tokens[start..=end].iter().cloned().collect();
        let record: ItemStruct = moxy::parse!(definition)?;
        selected.push(record);
    }
    let [record] = selected.as_slice() else {
        return Err("generated struct is missing or ambiguous".into());
    };
    if field_schema(record)? != field_schema(&expected)? {
        return Err("generated struct field schema changed".into());
    }
    let mut output = source.to_owned();
    output.replace_range(source_range(record.to_token_stream().span()), replacement);
    Ok(output)
}

fn field_schema(record: &ItemStruct) -> CodegenResult<Vec<(String, String, Vec<String>)>> {
    let fields = record.fields.as_named().ok_or("expected named fields")?;
    let mut schema = Vec::new();
    for field in fields.fields.iter() {
        let name = field
            .ident
            .as_ref()
            .ok_or("expected a field name")?
            .text()
            .to_owned();
        let mut attributes = Vec::new();
        for attribute in field
            .attrs
            .iter()
            .filter(|attribute| attribute.path.is_ident("prost"))
        {
            attributes.push(moxy::fmt!(attribute)?);
        }
        schema.push((name, moxy::fmt!(&field.ty)?, attributes));
    }
    schema.sort_unstable();
    Ok(schema)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use moxy::ast::{Attributed, File, ImplItem, Item};

    use super::*;

    fn annotated_methods(source: &str) -> Vec<(String, usize)> {
        let file: File = moxy::parse!(source).unwrap();
        let Item::Impl(implementation) = &file.items[0] else {
            panic!("expected impl")
        };
        implementation
            .items
            .iter()
            .filter_map(|member| {
                let ImplItem::Fn(method) = member else {
                    return None;
                };
                Some((
                    method.sig.ident.text().to_owned(),
                    method
                        .attrs
                        .iter()
                        .filter(|attr| attr.path.is_ident("must_use"))
                        .count(),
                ))
            })
            .collect()
    }

    fn token_values(tokens: &moxy::token::TokenStream) -> Vec<String> {
        let mut values = Vec::new();
        for token in tokens.iter() {
            if let moxy::token::TokenTree::Group(group) = token {
                values.push(format!("{:?}", group.delim));
                values.extend(token_values(&group.tokens));
                values.push("end group".to_owned());
            } else {
                values.push(token.to_string());
            }
        }
        values
    }

    #[test]
    fn enum_annotations_select_names_and_are_idempotent() {
        let source = "impl Mode { pub fn as_str_name(&self) -> &str { \"value\" } #[must_use] pub fn from_str_name(_: &str) -> Option<Self> { None } pub fn other() {} }";
        let once = annotate_must_use(source, MustUse::EnumNames, &[]).unwrap();
        let twice = annotate_must_use(&once, MustUse::EnumNames, &[]).unwrap();
        assert!(
            annotated_methods(&once)
                == vec![
                    ("as_str_name".into(), 1),
                    ("from_str_name".into(), 1),
                    ("other".into(), 0)
                ]
        );
        assert!(annotated_methods(&twice) == annotated_methods(&once));
    }

    #[test]
    fn public_method_policy_excludes_private_and_async_methods() {
        let source = "impl Service { pub fn build() {} fn private() {} pub async fn request() {} }";
        let updated = annotate_must_use(source, MustUse::PublicMethods, &[]).unwrap();
        assert!(
            annotated_methods(&updated)
                == vec![
                    ("build".into(), 1),
                    ("private".into(), 0),
                    ("request".into(), 0)
                ]
        );
    }

    #[test]
    fn builder_annotations_follow_nested_modules_and_exact_names() {
        let source = "mod service { pub struct ServiceBuilder; pub struct ServiceBuilderOptions; }";
        let updated = annotate_must_use(source, MustUse::EnumNames, &["ServiceBuilder"]).unwrap();
        let file: File = moxy::parse!(updated).unwrap();
        let Item::Mod(module) = &file.items[0] else {
            panic!("expected module")
        };
        let records = module.content.as_ref().unwrap();
        assert!(
            records[0]
                .attrs()
                .iter()
                .filter(|attr| attr.path.is_ident("must_use"))
                .count()
                == 1
        );
        assert!(
            !records[1]
                .attrs()
                .iter()
                .any(|attr| attr.path.is_ident("must_use"))
        );
    }

    #[test]
    fn compaction_preserves_literals_and_neighboring_items() {
        let source = "const PREFIX: &str = \"é\"; impl Trait for Profile { fn message() -> &'static str { \"}  spaced   { text\" } } const SUFFIX: u8 = 3;";
        let updated = compact_impl(source, "Trait", "Profile").unwrap();
        let original: File = moxy::parse!(source).unwrap();
        let compacted: File = moxy::parse!(updated).unwrap();
        assert!(original.items.len() == compacted.items.len());
        // Token comparisons check literal values and syntax, independently of
        // the source whitespace used to print each implementation.
        for (before, after) in original.items.iter().zip(&compacted.items) {
            assert!(
                token_values(&before.to_token_stream()) == token_values(&after.to_token_stream())
            );
        }
    }

    #[test]
    fn compaction_rejects_missing_and_ambiguous_implementations() {
        assert!(compact_impl("struct Profile;", "Trait", "Profile").is_err());
        assert!(
            compact_impl(
                "impl Trait for Profile {} impl Trait for Profile {}",
                "Trait",
                "Profile"
            )
            .is_err()
        );
    }

    #[test]
    fn struct_replacement_checks_schema_without_derive_or_field_order_dependency() {
        let expected = "#[derive(Clone, Copy)] pub struct Mapping { #[prost(uint64, tag = \"1\")] pub id: u64, #[prost(bool, tag = \"7\")] pub has_functions: bool }";
        let source = "#[derive(Copy, Clone)] pub struct Mapping { #[prost(bool, tag = \"7\")] pub has_functions: bool, #[prost(uint64, tag = \"1\")] pub id: u64 } struct Neighbor;";
        let replacement = "pub struct Mapping { pub id: u64, pub flags: u8 }";
        let updated = replace_struct(source, expected, replacement).unwrap();
        let file: File = moxy::parse!(updated).unwrap();
        let Item::Struct(mapping) = &file.items[0] else {
            panic!("expected mapping")
        };
        let names: Vec<_> = mapping
            .fields
            .as_named()
            .unwrap()
            .fields
            .iter()
            .map(|field| field.ident.as_ref().unwrap().text())
            .collect();
        assert!(names == ["id", "flags"]);
        assert!(file.items[1].as_struct().unwrap().ident.text() == "Neighbor");
        assert!(
            replace_struct("pub struct Mapping { pub id: i64 }", expected, replacement).is_err()
        );
        let changed_tag = "pub struct Mapping { #[prost(uint64, tag = \"2\")] pub id: u64, #[prost(bool, tag = \"7\")] pub has_functions: bool }";
        assert!(replace_struct(changed_tag, expected, replacement).is_err());
        assert!(replace_struct("struct Missing;", expected, replacement).is_err());
        assert!(replace_struct(&format!("{source} {source}"), expected, replacement).is_err());
    }
}
