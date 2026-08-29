#[allow(clippy::wildcard_imports)]
use super::*;

#[path = "analytics/index_patterns.rs"]
pub(crate) mod index_patterns;
pub(crate) use index_patterns::*;
#[path = "analytics/detected_fields.rs"]
pub(crate) mod detected_fields;
pub(crate) use detected_fields::*;
