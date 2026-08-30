use std::collections::BTreeMap;

use krabka_blockstore::Labels;
use serde_json::{Map, Value};

use super::format_sample_value;

// === split-modules: generated submodules ===
mod expand_alert_action;
mod expand_alert_mapping_json;
mod expand_alert_template;
mod labels_from_map;

use expand_alert_action::expand_alert_action;
pub (super) use expand_alert_mapping_json::expand_alert_mapping_json;
pub (crate) use expand_alert_template::expand_alert_template;
pub (super) use labels_from_map::labels_from_map;
