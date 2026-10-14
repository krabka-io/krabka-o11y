use std::collections::{BTreeMap, BTreeSet};

use krabka_metrics::{NativeHistogram, ResetHint};
use promql_parser::parser::{
    BinModifier, BinaryExpr, LabelModifier, VectorMatchCardinality,
    token::{
        T_ADD, T_ATAN2, T_DIV, T_EQLC, T_GTE, T_GTR, T_LAND, T_LOR, T_LSS, T_LTE, T_LUNLESS, T_MOD,
        T_MUL, T_NEQ, T_POW, T_SUB, TokenType,
    },
};

use super::{
    annotations::{emit_info, incompatible_types_in_binop_info, mismatched_custom_buckets_info},
    histogram::{
        add_compatible_native_histogram, scale_native_histogram_values, scaled_native_histogram,
    },
    labels::{
        float_sample_value, is_result_metadata_label, labels_key, labels_without_metric_name,
    },
    range_functions::native_histograms_equal,
};
use crate::{
    PromqlError, PromqlLabels as Labels,
    error::Result,
    result::{InstantSample, QueryResult, SampleValue},
};

mod append_left_fill_results;
mod apply_binary_fill_value;
mod apply_binary_sample_value;
mod apply_histogram_float_binary;
mod apply_histogram_histogram_binary;
mod binary_match_key;
mod binary_op;
mod binary_returns_bool;
mod combine_instant_binary;
mod copy_group_labels;
mod eval_many_to_one_vector_binary;
mod eval_one_to_many_vector_binary;
mod eval_one_to_one_vector_binary;
mod eval_vector_set_binary;
mod eval_vector_vector_binary;
mod fill_missing_right;
mod index_by_match_key;
mod instant_value;
mod left_sample_matches;
mod missing_side;
mod one_to_one_binary_result_labels;
mod scalar_side;
mod set_op;
mod validate_binary_modifier;
mod validate_set_modifier;
mod vector_operands;

use append_left_fill_results::{
    append_left_fill_results, append_matched_binary_result, append_right_fill_result,
    default_left_fill_labels,
};
use apply_binary_fill_value::apply_binary_fill_value;
use apply_binary_sample_value::apply_binary_sample_value;
use apply_histogram_float_binary::apply_histogram_float_binary;
use apply_histogram_histogram_binary::apply_histogram_histogram_binary;
use binary_match_key::{binary_match_key, set_binary_match_key};
use binary_op::BinaryOp;
use binary_returns_bool::binary_returns_bool;
pub(super) use combine_instant_binary::combine_instant_binary;
use copy_group_labels::copy_group_labels;
use eval_many_to_one_vector_binary::eval_many_to_one_vector_binary;
use eval_one_to_many_vector_binary::eval_one_to_many_vector_binary;
use eval_one_to_one_vector_binary::eval_one_to_one_vector_binary;
use eval_vector_set_binary::eval_vector_set_binary;
use eval_vector_vector_binary::eval_vector_vector_binary;
use fill_missing_right::fill_missing_right;
use index_by_match_key::index_by_match_key;
pub(super) use instant_value::InstantValue;
use left_sample_matches::{LeftSampleMatch, left_sample_matches};

fn visit_left_sample_matches<'a>(
    left: Vec<InstantSample>,
    right_by_key: &'a BTreeMap<String, InstantSample>,
    modifier: Option<&'a BinModifier>,
    mut visit: impl FnMut(LeftSampleMatch<'a>) -> Result<()>,
) -> Result<()> {
    for sample in left_sample_matches(left, right_by_key, modifier) {
        visit(sample)?;
    }
    Ok(())
}

struct LeftMatchResults {
    samples: Vec<InstantSample>,
    matched_keys: BTreeSet<String>,
}

fn append_left_match_results(
    left: Vec<InstantSample>,
    right_by_key: &BTreeMap<String, InstantSample>,
    matching: VectorMatching<'_>,
    reject_duplicate_left_keys: bool,
    mut right_fill_labels: impl FnMut(&InstantSample, bool) -> Labels,
    mut matched_labels: impl FnMut(&InstantSample, &InstantSample, bool) -> Labels,
) -> Result<LeftMatchResults> {
    let VectorMatching { op, modifier } = matching;
    let mut samples = Vec::new();
    let mut matched_keys = BTreeSet::new();
    visit_left_sample_matches(
        left,
        right_by_key,
        modifier,
        |LeftSampleMatch {
             left: left_sample,
             key,
             right,
         }| {
            let Some(right_sample) = right else {
                append_right_fill_result(&mut samples, &left_sample, matching, |preserves_name| {
                    right_fill_labels(&left_sample, preserves_name)
                })?;
                return Ok(());
            };
            if reject_duplicate_left_keys {
                if !matched_keys.insert(key.clone()) {
                    return Err(PromqlError::Exec(format!(
                        "many-to-one matching must be explicit for key `{key}`"
                    )));
                }
            } else {
                matched_keys.insert(key);
            }
            append_matched_binary_result(
                &mut samples,
                &left_sample,
                right_sample,
                op,
                modifier,
                |preserves_name| matched_labels(&left_sample, right_sample, preserves_name),
            )?;
            Ok(())
        },
    )?;
    Ok(LeftMatchResults {
        samples,
        matched_keys,
    })
}
use missing_side::MissingSide;
use one_to_one_binary_result_labels::one_to_one_binary_result_labels;
use scalar_side::ScalarSide;
use set_op::SetOp;
use validate_binary_modifier::validate_binary_modifier;
use validate_set_modifier::validate_set_modifier;
pub(super) use vector_operands::InstantOperands;
use vector_operands::{VectorMatching, VectorOperands};
