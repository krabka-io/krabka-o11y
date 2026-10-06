use krabka_domain_macros::EnumName;

#[derive(Clone, Copy, EnumName)]
pub enum Operation {
    #[name(value = "put_multipart")]
    PutMultipart,
    #[name(value = "block-builder")]
    BlockBuilder,
    #[name(value = "HTTP/2")]
    Http2,
    #[name(value = "quoted \"name\"\nnext line")]
    Escaped,
}

#[derive(EnumName)]
#[repr(u8)]
enum NonCopy {
    #[name(value = "first")]
    First = 17,
    #[name(value = "second")]
    Second = 31,
}

const OPERATION_NAME: &str = Operation::PutMultipart.as_str();
const NON_COPY_NAME: &str = NonCopy::First.as_str();

#[test]
fn preserves_explicit_spelling_in_const_accessor_and_display() {
    assert2::assert!(OPERATION_NAME == "put_multipart");
    for (operation, expected) in [
        (Operation::PutMultipart, "put_multipart"),
        (Operation::BlockBuilder, "block-builder"),
        (Operation::Http2, "HTTP/2"),
        (Operation::Escaped, "quoted \"name\"\nnext line"),
    ] {
        assert2::assert!(operation.as_str() == expected);
        assert2::assert!(operation.to_string() == expected);
    }
}

#[test]
fn display_does_not_consume_non_copy_variants() {
    assert2::assert!(NON_COPY_NAME == "first");
    let value = NonCopy::Second;
    assert2::assert!(value.to_string() == "second");
    assert2::assert!(value.as_str() == "second");
}
