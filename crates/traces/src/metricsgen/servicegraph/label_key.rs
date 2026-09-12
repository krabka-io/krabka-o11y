use super::ConnectionType;

pub(crate) type LabelKey = (String, String, ConnectionType, Vec<(String, String)>);
