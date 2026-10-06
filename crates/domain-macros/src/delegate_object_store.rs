use moxy::{
    ast::{Expr, ImplItem, ItemImpl, ParseError},
    diagnostic::SpanExt,
    token::{Delim, Spanner, ToTokens, TokenStream, TokenTree},
};

pub fn expand(meta: TokenStream, tokens: TokenStream) -> Result<TokenStream, ParseError> {
    if meta.is_empty() {
        return meta
            .span()
            .error("delegate_object_store requires a delegate expression")
            .into();
    }
    let _: Expr = moxy::parse!(meta)?;
    let delegate = meta;
    let inspection = signature_tokens(tokens.clone());
    let item: ItemImpl = moxy::parse!(inspection)?;
    let object_store = item.trait_ref.as_ref().is_some_and(|trait_ref| {
        trait_ref.polarity.is_none()
            && trait_ref
                .path
                .last()
                .is_some_and(|segment| segment.ident == "ObjectStore")
    });
    if !object_store {
        return item
            .span()
            .error("delegate_object_store requires an ObjectStore impl")
            .into();
    }
    if item
        .items
        .iter()
        .any(|member| matches!(member, ImplItem::Macro(_)))
    {
        return item
            .span()
            .error("delegate_object_store requires explicit methods, not method macros")
            .into();
    }
    let forwarding_tokens = moxy::template! {
        impl ObjectStore for Placeholder {
            async fn put_opts(
                &self,
                location: &::object_store::path::Path,
                payload: ::object_store::PutPayload,
                options: ::object_store::PutOptions,
            ) -> ::object_store::Result<::object_store::PutResult> {
                ({{ &delegate }}).put_opts(location, payload, options).await
            }

            async fn put_multipart_opts(
                &self,
                location: &::object_store::path::Path,
                options: ::object_store::PutMultipartOptions,
            ) -> ::object_store::Result<::std::boxed::Box<dyn ::object_store::MultipartUpload>> {
                ({{ &delegate }}).put_multipart_opts(location, options).await
            }

            async fn get_opts(
                &self,
                location: &::object_store::path::Path,
                options: ::object_store::GetOptions,
            ) -> ::object_store::Result<::object_store::GetResult> {
                ({{ &delegate }}).get_opts(location, options).await
            }

            fn list(
                &self,
                prefix: ::core::option::Option<&::object_store::path::Path>,
            ) -> ::futures::stream::BoxStream<'static, ::object_store::Result<::object_store::ObjectMeta>> {
                ({{ &delegate }}).list(prefix)
            }

            async fn list_with_delimiter(
                &self,
                prefix: ::core::option::Option<&::object_store::path::Path>,
            ) -> ::object_store::Result<::object_store::ListResult> {
                ({{ &delegate }}).list_with_delimiter(prefix).await
            }

            async fn copy_opts(
                &self,
                from: &::object_store::path::Path,
                to: &::object_store::path::Path,
                options: ::object_store::CopyOptions,
            ) -> ::object_store::Result<()> {
                ({{ &delegate }}).copy_opts(from, to, options).await
            }

            fn delete_stream(
                &self,
                locations: ::futures::stream::BoxStream<'static, ::object_store::Result<::object_store::path::Path>>,
            ) -> ::futures::stream::BoxStream<'static, ::object_store::Result<::object_store::path::Path>> {
                ({{ &delegate }}).delete_stream(locations)
            }
        }
    };
    let forwarding: ItemImpl = moxy::parse!(forwarding_tokens)?;
    let mut missing = TokenStream::new();
    for member in forwarding.items.inner {
        let Some(method) = member.as_fn() else {
            continue;
        };
        if let Some(existing) = item
            .items
            .iter()
            .filter_map(ImplItem::as_fn)
            .find(|existing| existing.sig.ident.text() == method.sig.ident.text())
        {
            if existing.attrs.iter().any(|attribute| {
                attribute.path.is_ident("cfg") || attribute.path.is_ident("cfg_attr")
            }) {
                return existing
                    .span()
                    .error("delegate_object_store does not support conditional required methods")
                    .into();
            }
        } else {
            member.to_tokens(&mut missing);
        }
    }
    // Inspect methods with the AST, but retain every user-written token.
    // Re-rendering a parsed body can alter syntax that the AST does not preserve.
    let mut output = tokens.into_inner();
    let Some(TokenTree::Group(body)) = output.last_mut() else {
        return item
            .span()
            .error("delegate_object_store requires an impl body")
            .into();
    };
    if body.delim != Delim::Brace {
        return item
            .span()
            .error("delegate_object_store requires an impl body")
            .into();
    }
    body.tokens.extend(missing);
    Ok(output.into_iter().collect())
}

// Keep names and attributes for inspection. Discard parameters, return types,
// and bodies so the AST parser cannot hide an override it does not understand.
fn signature_tokens(tokens: TokenStream) -> TokenStream {
    let mut output = tokens.into_inner();
    if let Some(TokenTree::Group(body)) = output.last_mut()
        && body.delim == Delim::Brace
    {
        let mut inspection = TokenStream::new();
        let mut header: Vec<TokenTree> = Vec::new();
        for token in body.tokens.clone() {
            if matches!(&token, TokenTree::Group(group) if group.delim == Delim::Brace) {
                if let Some(index) = header.iter().position(|token| token.text() == Some("fn")) {
                    header.truncate(index + 2);
                    inspection.extend(header.drain(..));
                    inspection.extend(moxy::template! { () {} });
                } else {
                    inspection.extend(header.drain(..));
                    inspection.extend_one(token);
                }
            } else {
                header.push(token);
            }
        }
        inspection.extend(header);
        body.tokens = inspection;
    }
    output.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_delegate_expressions() {
        for meta in ["", "self.inner, self.other"] {
            let meta = meta.parse::<TokenStream>().unwrap();
            let tokens = moxy::template! { impl ObjectStore for Store {} };
            assert2::assert!(expand(meta, tokens).is_err());
        }
    }

    #[test]
    fn rejects_non_store_impls_and_method_macros() {
        for source in [
            "impl Store {}",
            "impl Display for Store {}",
            "impl ObjectStore for Store { methods!(); }",
            "impl ObjectStore for Store { #[cfg(feature = \"optional\")] fn list() {} }",
            "impl ObjectStore for Store { #[cfg_attr(feature = \"optional\", cfg(any()))] fn list() {} }",
        ] {
            let meta = moxy::template! { self.inner };
            let tokens = source.parse::<TokenStream>().unwrap();
            assert2::assert!(expand(meta, tokens).is_err());
        }
    }
}
