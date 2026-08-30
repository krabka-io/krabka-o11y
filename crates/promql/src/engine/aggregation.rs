use std::{cmp::Ordering, collections::BTreeMap};

use krabka_blockstore::Labels;
use krabka_metrics::NativeHistogram;
#[cfg(feature = "experimental-functions")]
use num_traits::ToPrimitive as _;
use promql_parser::parser::{
    AggregateExpr, Expr, LabelModifier,
    token::{T_TOPK, TokenType},
};

use super::{
    annotations::{emit_warning, invalid_quantile_warning, is_valid_quantile},
    histogram::{add_compatible_native_histogram, scaled_native_histogram},
    labels::{aggregate_labels, float_sample_value, labels_key},
    range_functions::kahan_sum_inc,
    result_utils::quantile_value,
};
use crate::{
    error::{PromqlError, Result},
    result::{InstantSample, SampleValue},
};

#[cfg(all(test, feature = "experimental-functions"))]
mod tests {
    use super::*;

    #[test]
    fn limit_ratio_uses_a_strict_positive_hash_threshold() {
        let mut labels = Labels::new();
        labels.insert("__name__", "requests_total");
        labels.insert("instance", "api-1");
        let offset = prometheus_labels_hash(&labels).to_f64().unwrap() / u64::MAX.to_f64().unwrap();

        assert!(!limit_ratio_includes_sample(offset, &labels));
        assert!(limit_ratio_includes_sample(offset.next_up(), &labels));
        assert!(!limit_ratio_includes_sample(0.0, &labels));
        assert!(!limit_ratio_includes_sample(-0.0, &labels));
    }
}

// === split-modules: generated submodules ===
mod aggregate_k;
mod aggregate_op;
mod aggregate_quantile;
mod aggregate_state;
mod apply_count_values_aggregate;
mod apply_k_aggregate;
mod apply_limit_ratio_aggregate;
mod apply_limitk_aggregate;
mod apply_quantile_aggregate;
mod apply_simple_aggregate;
mod apply_stddev_stdvar_aggregate;
mod compare_k_aggregate_samples;
mod count_values_label_value;
mod limit_ratio_includes_sample;
mod prometheus_labels_hash;

pub(super) use aggregate_k::aggregate_k;
pub(super) use aggregate_op::AggregateOp;
pub(super) use aggregate_quantile::aggregate_quantile;
use aggregate_state::AggregateState;
pub(super) use apply_count_values_aggregate::apply_count_values_aggregate;
pub(super) use apply_k_aggregate::apply_k_aggregate;
#[cfg(feature = "experimental-functions")]
pub(super) use apply_limit_ratio_aggregate::apply_limit_ratio_aggregate;
#[cfg(feature = "experimental-functions")]
pub(super) use apply_limitk_aggregate::apply_limitk_aggregate;
pub(super) use apply_quantile_aggregate::apply_quantile_aggregate;
pub(super) use apply_simple_aggregate::apply_simple_aggregate;
pub(super) use apply_stddev_stdvar_aggregate::apply_stddev_stdvar_aggregate;
use compare_k_aggregate_samples::compare_k_aggregate_samples;
use count_values_label_value::count_values_label_value;
#[cfg(feature = "experimental-functions")]
use limit_ratio_includes_sample::limit_ratio_includes_sample;
#[cfg(feature = "experimental-functions")]
use prometheus_labels_hash::prometheus_labels_hash;
