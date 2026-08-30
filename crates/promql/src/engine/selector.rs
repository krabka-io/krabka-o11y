use std::{collections::BTreeSet, time::SystemTime};

use krabka_blockstore::{LabelMatcher, Labels, MatchOp};
use krabka_units::prelude::*;
use num_traits::ToPrimitive;
use promql_parser::{
    label as prom_label,
    parser::{AtModifier, Offset, VectorSelector},
};
use regex::Regex;

use crate::{PromqlError, error::Result};

// === split-modules: generated submodules ===
mod apply_offset_delta;
mod apply_selector_time_modifier;
mod at_modifier_bounds;
mod build_label_matchers;
mod compile_label_matchers;
mod compiled_label_matcher;
mod compiled_label_matchers;
mod duration_to_i64_ms;
mod info_data_label_matchers;
mod label_matcher_sets;
mod labels_match;
mod selector_at_ms;
mod selector_duration;
mod selector_offset;
mod system_time_ms;
mod timestamp_seconds;

use apply_offset_delta::apply_offset_delta;
pub(super) use apply_selector_time_modifier::apply_selector_time_modifier;
pub(super) use at_modifier_bounds::AtModifierBounds;
use build_label_matchers::build_label_matchers;
pub(super) use compile_label_matchers::compile_label_matchers;
use compiled_label_matcher::CompiledLabelMatcher;
pub(super) use compiled_label_matchers::CompiledLabelMatchers;
use duration_to_i64_ms::duration_to_i64_ms;
pub(super) use info_data_label_matchers::info_data_label_matchers;
pub(crate) use label_matcher_sets::label_matcher_sets;
pub(super) use labels_match::labels_match;
use selector_at_ms::selector_at_ms;
pub(super) use selector_duration::selector_duration;
use selector_offset::selector_offset;
use system_time_ms::system_time_ms;
pub(super) use timestamp_seconds::timestamp_seconds;
