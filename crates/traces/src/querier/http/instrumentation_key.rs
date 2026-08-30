use super::AttrValue;

pub(crate) type InstrumentationKey = (String, String, Vec<(String, AttrValue)>);
