use super::{Display, From, Into};

/// The JSON path expression an extraction reads from, for example
/// `request.headers[0]`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct JsonExpressionPath(pub String);
