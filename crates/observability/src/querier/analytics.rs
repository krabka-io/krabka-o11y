use super::*;

#[path = "analytics/index_patterns.rs"]
pub(crate) mod index_patterns;
pub use index_patterns::*;
#[path = "analytics/detected_fields.rs"]
pub(crate) mod detected_fields;
pub use detected_fields::*;
