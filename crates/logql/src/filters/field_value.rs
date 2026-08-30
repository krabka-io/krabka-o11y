use super::{ByteSize, IpMatcher};

#[derive(Clone, Debug, PartialEq)]
pub enum FieldValue {
    Number(f64),
    Duration(i64),
    Bytes(ByteSize),
    String(String),
    Ip(IpMatcher),
}
