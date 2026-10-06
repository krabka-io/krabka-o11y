use clap::ValueEnum;
use krabka_domain_macros::EnumName;

#[derive(Clone, Copy, Debug, PartialEq, Eq, EnumName)]
#[enum_name(parse)]
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
#[enum_name(parse)]
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
        assert2::assert!(Operation::from_name(expected) == Some(operation));
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
    assert2::assert!(matches!(
        NonCopy::from_name("second"),
        Some(NonCopy::Second)
    ));
}

#[derive(Clone, Copy, clap::ValueEnum, EnumName)]
#[enum_name(clap, parse)]
enum Protocol {
    #[value(name = "SASL_SSL")]
    SaslSsl,
    #[value(name = "GSSAPI", hide = true)]
    Gssapi,
    #[value(name = "PLAIN")]
    Plain,
}

const PROTOCOL_NAME: &str = Protocol::SaslSsl.as_str();

#[test]
fn clap_names_match_parsing_const_access_and_display() {
    assert2::assert!(PROTOCOL_NAME == "SASL_SSL");
    for (protocol, expected) in [
        (Protocol::SaslSsl, "SASL_SSL"),
        (Protocol::Gssapi, "GSSAPI"),
        (Protocol::Plain, "PLAIN"),
    ] {
        assert2::assert!(Protocol::from_name(expected).unwrap().as_str() == expected);
        assert2::assert!(protocol.as_str() == expected);
        assert2::assert!(protocol.to_string() == expected);
        assert2::assert!(Protocol::from_str(expected, false).unwrap().as_str() == expected);
    }
    assert2::assert!(Protocol::Gssapi.to_possible_value().unwrap().is_hide_set());
    assert2::assert!(Protocol::from_str("sasl_ssl", false).is_err());
}

#[test]
fn parsing_preserves_case_punctuation_and_non_copy_discriminants() {
    assert2::assert!(matches!(NonCopy::from_name("first"), Some(NonCopy::First)));
    assert2::assert!(matches!(
        NonCopy::from_name("second"),
        Some(NonCopy::Second)
    ));
    for name in ["", "unknown", "http/2", "HTTP/2 ", "put", "put-multipart"] {
        assert2::assert!(Operation::from_name(name).is_none(), "{name}");
    }
    assert2::assert!(Protocol::from_name("sasl_ssl").is_none());
}
