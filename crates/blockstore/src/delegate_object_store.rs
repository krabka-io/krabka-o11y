/// Implements [`object_store::ObjectStore`] for a wrapper store, forwarding
/// the listed methods to one of its fields.
///
/// A store that injects a fault, counts calls or records payloads overrides
/// one or two operations and passes every other one through. This writes the
/// pass-through half: name the wrapper type, the field that holds the inner
/// store, and the methods to forward, then give the overriding methods as
/// ordinary `async fn` / `fn` items. Every required trait method must be
/// either forwarded or overridden.
///
/// The forwardable methods are `put_opts`, `put_multipart_opts`, `get_opts`,
/// `get_ranges`, `delete_stream`, `list`, `list_with_delimiter` and
/// `copy_opts`. The expansion names `::async_trait`, `::object_store`,
/// `::futures` and `::bytes`, so the calling crate depends on all four.
///
/// ```ignore
/// krabka_blockstore::delegate_object_store! {
///     CountingStore => inner;
///     forward [put_multipart_opts, get_opts, delete_stream, list, list_with_delimiter, copy_opts];
///
///     async fn put_opts(
///         &self,
///         location: &Path,
///         payload: PutPayload,
///         options: PutOptions,
///     ) -> object_store::Result<PutResult> {
///         self.puts.fetch_add(1, Ordering::SeqCst);
///         self.inner.put_opts(location, payload, options).await
///     }
/// }
/// ```
#[macro_export]
macro_rules! delegate_object_store {
    (
        $wrapper:ty => $inner:ident;
        forward [$($method:ident),* $(,)?];
        $($overrides:tt)*
    ) => {
        $crate::delegate_object_store!(
            @munch $wrapper, $inner, [$($overrides)*], [], [$($method)*]
        );
    };
    (@munch $wrapper:ty, $inner:ident, [$($overrides:tt)*], [$($done:tt)*], []) => {
        #[::async_trait::async_trait]
        impl ::object_store::ObjectStore for $wrapper {
            $($overrides)*
            $($done)*
        }
    };
    (@munch $w:ty, $i:ident, [$($o:tt)*], [$($d:tt)*], [put_opts $($rest:ident)*]) => {
        $crate::delegate_object_store!(@munch $w, $i, [$($o)*], [$($d)*
            async fn put_opts(
                &self,
                location: &::object_store::path::Path,
                payload: ::object_store::PutPayload,
                options: ::object_store::PutOptions,
            ) -> ::object_store::Result<::object_store::PutResult> {
                self.$i.put_opts(location, payload, options).await
            }
        ], [$($rest)*]);
    };
    (@munch $w:ty, $i:ident, [$($o:tt)*], [$($d:tt)*], [put_multipart_opts $($rest:ident)*]) => {
        $crate::delegate_object_store!(@munch $w, $i, [$($o)*], [$($d)*
            async fn put_multipart_opts(
                &self,
                location: &::object_store::path::Path,
                options: ::object_store::PutMultipartOptions,
            ) -> ::object_store::Result<Box<dyn ::object_store::MultipartUpload>> {
                self.$i.put_multipart_opts(location, options).await
            }
        ], [$($rest)*]);
    };
    (@munch $w:ty, $i:ident, [$($o:tt)*], [$($d:tt)*], [get_opts $($rest:ident)*]) => {
        $crate::delegate_object_store!(@munch $w, $i, [$($o)*], [$($d)*
            async fn get_opts(
                &self,
                location: &::object_store::path::Path,
                options: ::object_store::GetOptions,
            ) -> ::object_store::Result<::object_store::GetResult> {
                self.$i.get_opts(location, options).await
            }
        ], [$($rest)*]);
    };
    (@munch $w:ty, $i:ident, [$($o:tt)*], [$($d:tt)*], [get_ranges $($rest:ident)*]) => {
        $crate::delegate_object_store!(@munch $w, $i, [$($o)*], [$($d)*
            async fn get_ranges(
                &self,
                location: &::object_store::path::Path,
                ranges: &[::std::ops::Range<u64>],
            ) -> ::object_store::Result<Vec<::bytes::Bytes>> {
                self.$i.get_ranges(location, ranges).await
            }
        ], [$($rest)*]);
    };
    (@munch $w:ty, $i:ident, [$($o:tt)*], [$($d:tt)*], [delete_stream $($rest:ident)*]) => {
        $crate::delegate_object_store!(@munch $w, $i, [$($o)*], [$($d)*
            fn delete_stream(
                &self,
                locations: ::futures::stream::BoxStream<
                    'static,
                    ::object_store::Result<::object_store::path::Path>,
                >,
            ) -> ::futures::stream::BoxStream<
                'static,
                ::object_store::Result<::object_store::path::Path>,
            > {
                self.$i.delete_stream(locations)
            }
        ], [$($rest)*]);
    };
    (@munch $w:ty, $i:ident, [$($o:tt)*], [$($d:tt)*], [list $($rest:ident)*]) => {
        $crate::delegate_object_store!(@munch $w, $i, [$($o)*], [$($d)*
            fn list(
                &self,
                prefix: Option<&::object_store::path::Path>,
            ) -> ::futures::stream::BoxStream<
                'static,
                ::object_store::Result<::object_store::ObjectMeta>,
            > {
                self.$i.list(prefix)
            }
        ], [$($rest)*]);
    };
    (@munch $w:ty, $i:ident, [$($o:tt)*], [$($d:tt)*], [list_with_delimiter $($rest:ident)*]) => {
        $crate::delegate_object_store!(@munch $w, $i, [$($o)*], [$($d)*
            async fn list_with_delimiter(
                &self,
                prefix: Option<&::object_store::path::Path>,
            ) -> ::object_store::Result<::object_store::ListResult> {
                self.$i.list_with_delimiter(prefix).await
            }
        ], [$($rest)*]);
    };
    (@munch $w:ty, $i:ident, [$($o:tt)*], [$($d:tt)*], [copy_opts $($rest:ident)*]) => {
        $crate::delegate_object_store!(@munch $w, $i, [$($o)*], [$($d)*
            async fn copy_opts(
                &self,
                from: &::object_store::path::Path,
                to: &::object_store::path::Path,
                options: ::object_store::CopyOptions,
            ) -> ::object_store::Result<()> {
                self.$i.copy_opts(from, to, options).await
            }
        ], [$($rest)*]);
    };
}
