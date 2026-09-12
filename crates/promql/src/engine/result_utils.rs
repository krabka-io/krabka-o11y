use std::collections::BTreeSet;

use num_traits::ToPrimitive;

use super::labels::labels_key;
use crate::{PromqlError, error::Result, result::QueryResult};

mod finalize_metric_names;
mod quantile_value;
mod validate_unique_instant_labelsets;

pub(super) use finalize_metric_names::finalize_metric_names;
pub(super) use quantile_value::quantile_value;
pub(super) use validate_unique_instant_labelsets::validate_unique_instant_labelsets;
