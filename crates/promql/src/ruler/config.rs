use std::collections::BTreeMap;

use krabka_units::prelude::*;

use crate::PromqlError;

// === split-modules: generated submodules ===
mod parse_duration;
mod stable_hash_parts;
mod yaml_duration;
mod yaml_optional_string;
mod yaml_required_string;
mod yaml_string_map;

pub(super) use parse_duration::parse_duration;
pub(super) use stable_hash_parts::stable_hash_parts;
pub(super) use yaml_duration::yaml_duration;
pub(super) use yaml_optional_string::yaml_optional_string;
pub(super) use yaml_required_string::yaml_required_string;
pub(super) use yaml_string_map::yaml_string_map;
