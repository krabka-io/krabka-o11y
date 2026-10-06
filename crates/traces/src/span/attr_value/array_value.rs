//! Arrays use an OTLP payload inside native records. This keeps recursive
//! attributes out of serde-wincode's nested reader and writer type expansion.
use opentelemetry_proto::tonic::common::v1::ArrayValue;
use prost::Message as _;
use serde::{Deserialize as _, Deserializer, Serialize as _, Serializer, de::Error as _};

use super::AttrValue;

pub(super) fn serialize<S: Serializer>(
    values: &[AttrValue],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    ArrayValue {
        values: values.iter().map(AttrValue::otlp_value).collect(),
    }
    .encode_to_vec()
    .serialize(serializer)
}

pub(super) fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<AttrValue>, D::Error> {
    let bytes = Vec::<u8>::deserialize(deserializer)?;
    let array = ArrayValue::decode(bytes.as_slice()).map_err(D::Error::custom)?;
    Ok(array.values.iter().map(AttrValue::from).collect())
}
