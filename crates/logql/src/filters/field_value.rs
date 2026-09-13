use super::{ByteSize, IpMatcher};

#[derive(Clone, Debug, PartialEq)]
/// A typed value used by a LogQL field filter.
pub enum FieldValue {
    Number(f64),
    Duration(i64),
    Bytes(ByteSize),
    String(String),
    Ip(IpMatcher),
}
