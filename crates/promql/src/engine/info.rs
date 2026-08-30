use std::collections::{BTreeMap, BTreeSet};

use krabka_blockstore::{LabelMatcher, Labels};
use promql_parser::parser::{Call, Expr, VectorSelector};

use super::selector::{compile_label_matchers, info_data_label_matchers, labels_match};
use crate::{
    PromqlError,
    error::Result,
    result::{InstantSample, SampleValue},
};

// === split-modules: generated submodules ===
mod apply_info;
mod info_context;
mod info_identifying_key;
mod info_samples_by_identifying_key;
mod parse_info_call;

pub(super) use apply_info::apply_info;
pub(super) use info_context::InfoContext;
use info_identifying_key::info_identifying_key;
pub(super) use info_samples_by_identifying_key::info_samples_by_identifying_key;
pub(super) use parse_info_call::parse_info_call;
