use moxy::{
    ast::{Cursor, Declaration, Parse, ParseError, Parser},
    diagnostic::SpanExt,
    token::{Delim, Spanner, TokenStream, TokenTree},
};

pub struct Input(pub TokenStream);

impl Parse for Input {
    fn peek(cursor: Cursor<'_>) -> bool {
        !cursor.is_empty()
    }

    fn parse(parser: &Parser) -> Result<Self, ParseError> {
        Ok(Self(
            std::iter::from_fn(|| parser.advance().cloned()).collect(),
        ))
    }

    fn skip(cursor: Cursor<'_>) -> Option<Cursor<'_>> {
        Some(cursor.offset(cursor.remaining()))
    }
}

pub fn expand(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    if !tokens.iter().any(|token| token.text() == Some("struct")) {
        return tokens
            .span()
            .error("TypeNameDisplay requires a struct")
            .into();
    }
    let header = header_tokens(tokens);
    let declaration: Declaration = moxy::parse!(header)?;
    let Declaration::Struct(item) = declaration else {
        return declaration
            .span()
            .error("TypeNameDisplay requires a struct")
            .into();
    };
    let (impl_generics, type_generics, where_clause) = item.generics.split();
    let name = item.ident.text().to_owned();
    Ok(moxy::template! {
        impl {{ impl_generics }} ::core::fmt::Display
            for {{ &item.ident }} {{ type_generics }} {{ where_clause }}
        {
            fn fmt(&self, formatter: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                formatter.write_str({{ name }})
            }
        }
    })
}

fn header_tokens(tokens: TokenStream) -> TokenStream {
    let mut tokens = tokens.into_inner();
    let Some(struct_index) = tokens
        .iter()
        .position(|token| token.text() == Some("struct"))
    else {
        return tokens.into_iter().collect();
    };
    // Field types do not affect this derive. Keep their group and span, but do
    // not ask the declaration parser to inspect syntax that it does not use.
    if let Some(TokenTree::Group(fields)) = tokens.last_mut()
        && fields.delim == Delim::Brace
    {
        fields.tokens = TokenStream::new();
    } else {
        let mut depth = 0_usize;
        let mut arrow = false;
        let mut tuple_fields = None;
        for (index, token) in tokens.iter().enumerate().skip(struct_index + 2) {
            match token {
                TokenTree::Punct(punct) => {
                    match punct.as_str() {
                        "<" => depth += 1,
                        ">" if !arrow => depth = depth.saturating_sub(1),
                        _ => {}
                    }
                    arrow = punct.as_str() == "-";
                }
                TokenTree::Group(fields) if depth == 0 && fields.delim == Delim::Paren => {
                    tuple_fields = Some(index);
                    break;
                }
                _ if depth == 0 && token.text() == Some("where") => break,
                _ => arrow = false,
            }
        }
        if let Some(index) = tuple_fields {
            tokens.remove(index);
            tokens.pop();
            // Tuple structs put their where clause after the fields. An empty
            // named body lets the parser read that clause as part of the header.
            tokens.extend(moxy::template! { {} });
        }
    }
    tokens.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_enums_and_unions() {
        for source in ["enum Value { Empty }", "union Value { value: u8 }"] {
            let tokens = source.parse::<TokenStream>().unwrap();
            assert2::assert!(expand(tokens).is_err(), "{source}");
        }
    }
}
