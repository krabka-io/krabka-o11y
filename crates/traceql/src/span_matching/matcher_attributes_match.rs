use super::{SpanMatcher, attr_values_match};
use crate::result::Attribute;

/// Whether the values `attributes` holds under `matcher.key` satisfy the
/// matcher's operator and operand.
///
/// This ignores `matcher.negated`, and reads every attribute regardless of
/// `matcher.scope`; the caller picks the attribute list the scope names.
#[must_use]
pub fn matcher_attributes_match(attributes: &[Attribute], matcher: &SpanMatcher) -> bool {
    let values = attributes
        .iter()
        .filter(|attribute| attribute.key == matcher.key)
        .map(|attribute| &attribute.value)
        .collect::<Vec<_>>();
    attr_values_match(&values, matcher.op, &matcher.value)
}
