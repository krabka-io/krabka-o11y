use std::collections::{BTreeMap, BTreeSet};

use krabka_blockstore::{LabelMatcher, Labels, MatchOp};
use promql_parser::parser::{Expr, LabelModifier, VectorSelector};

use super::selector::label_matcher_sets;
use crate::{
    PromqlError,
    error::Result,
    result::{InstantSample, SampleValue},
};

// === split-modules: generated submodules ===
mod absent_labels;
mod absent_labels_from_matchers;
mod absent_labels_from_selector;
mod aggregate_labels;
mod float_sample_value;
mod is_result_metadata_label;
mod labels_key;
mod labels_without_metric_and_label;
mod labels_without_metric_name;
mod record_metric_name;

pub(super) use absent_labels::absent_labels;
use absent_labels_from_matchers::absent_labels_from_matchers;
use absent_labels_from_selector::absent_labels_from_selector;
pub(super) use aggregate_labels::aggregate_labels;
pub(super) use float_sample_value::float_sample_value;
pub(super) use is_result_metadata_label::is_result_metadata_label;
pub(super) use labels_key::labels_key;
pub(super) use labels_without_metric_and_label::labels_without_metric_and_label;
pub(super) use labels_without_metric_name::labels_without_metric_name;
pub(super) use record_metric_name::record_metric_name;
