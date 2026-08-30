use std::collections::BTreeMap;

use datafusion::{logical_expr::LogicalPlan, prelude::SessionContext};
use krabka_blockstore::{Labels, SeriesFingerprint};

use crate::result::{InstantSample, RangeSeries};

// === split-modules: generated submodules ===
mod instant_shape;
mod operator_instant;
mod planned_instant;

pub (super) use instant_shape::InstantShape;
pub (super) use operator_instant::OperatorInstant;
pub (super) use planned_instant::PlannedInstant;
