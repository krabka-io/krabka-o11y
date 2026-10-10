//! How a [`SpanMatcher`] compares against attribute values, events and
//! links, and how nested event and link tags list their values.
//!
//! Every `SpanStore` evaluates matchers with these functions, so the
//! in-memory store and the block-backed stores agree on `TraceQL` semantics.

use std::collections::BTreeSet;

use krabka_units::{Time, convert::TimeExt};

use crate::{
    engine::bytes_to_hex,
    result::{AttrValue, EventRef, LinkRef, SpanRef},
    span_columns::InputSpan,
    store::{MatchCmp, MatchScope, MatchValue, SpanMatcher},
};

mod attr_matches;
mod attr_values_match;
mod bool_matches;
mod collect_event_values;
mod collect_link_values;
mod collect_span_field_values;
mod enum_int_matches;
mod event_matcher_matches_absence;
mod event_matcher_matches_event;
mod float_matches;
mod int_matches;
mod link_matcher_matches_absence;
mod link_matcher_matches_link;
mod matcher_attributes_match;
mod nested_attribute_key_matches;
mod nested_presence_matches;
mod nil_matches;
mod present_value_matches;
mod span_intrinsic_fields;
mod string_matches;
mod typed_value_parts;

pub use attr_matches::attr_matches;
pub use attr_values_match::attr_values_match;
pub use bool_matches::bool_matches;
pub use collect_event_values::collect_event_values;
pub use collect_link_values::collect_link_values;
pub use collect_span_field_values::collect_span_field_values;
pub use enum_int_matches::enum_int_matches;
pub use event_matcher_matches_absence::event_matcher_matches_absence;
pub use event_matcher_matches_event::event_matcher_matches_event;
pub use float_matches::float_matches;
pub use int_matches::int_matches;
pub use link_matcher_matches_absence::link_matcher_matches_absence;
pub use link_matcher_matches_link::link_matcher_matches_link;
pub use matcher_attributes_match::matcher_attributes_match;
use nested_attribute_key_matches::nested_attribute_key_matches;
pub use nested_presence_matches::nested_presence_matches;
pub use nil_matches::nil_matches;
pub use present_value_matches::present_value_matches;
pub use span_intrinsic_fields::SpanIntrinsicFields;
pub use string_matches::string_matches;
pub use typed_value_parts::typed_value_parts;
