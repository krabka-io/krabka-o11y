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

#[derive(Clone, Copy)]
pub(crate) enum MissingSide {
    Left,
    Right,
}

#[derive(Clone, Copy)]
pub(crate) enum ScalarSide {
    Left,
    Right,
}

pub(super) enum InstantValue {
    Scalar(f64),
    Vector(Vec<InstantSample>),
}

#[cfg(test)]
impl InstantValue {
    pub(crate) fn try_from_query(result: QueryResult) -> Result<Self> {
        match result {
            QueryResult::Scalar { value, .. } => Ok(Self::Scalar(value)),
            QueryResult::InstantVector(samples) => Ok(Self::Vector(samples)),
            QueryResult::RangeMatrix(_) => Err(PromqlError::Plan(
                "binary expression requires instant operands".to_string(),
            )),
            QueryResult::Str { .. } => Err(PromqlError::Plan(
                "binary expression does not support string operands".to_string(),
            )),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum SetOp {
    And,
    Or,
    Unless,
}

impl SetOp {
    pub(crate) fn from_token(token: TokenType) -> Option<Self> {
        match token.id() {
            T_LAND => Some(Self::And),
            T_LOR => Some(Self::Or),
            T_LUNLESS => Some(Self::Unless),
            _ => None,
        }
    }
}

/// The two instant vectors of a vector-to-vector binary operation.
pub(crate) struct VectorOperands {
    pub(crate) left: Vec<InstantSample>,
    pub(crate) right: Vec<InstantSample>,
}

/// The operator of a vector-to-vector operation and how it matches.
#[derive(Clone, Copy)]
pub(crate) struct VectorMatching<'a> {
    pub(crate) op: BinaryOp,
    pub(crate) modifier: Option<&'a BinModifier>,
}

/// The two evaluated operands of an instant binary expression.
pub(super) struct InstantOperands {
    pub(crate) lhs: InstantValue,
    pub(crate) rhs: InstantValue,
}

mod append_left_fill_results;
mod apply_binary_sample_value;
mod apply_histogram_float_binary;
mod apply_histogram_histogram_binary;
mod binary_match_key;
mod binary_op;
mod combine_instant_binary;
mod copy_group_labels;
mod eval_many_to_one_vector_binary;
mod eval_one_to_many_vector_binary;
mod eval_one_to_one_vector_binary;
mod eval_vector_set_binary;
mod fill_missing_right;
mod left_sample_matches;
mod one_to_one_binary_result_labels;

use append_left_fill_results::{
    append_left_fill_results, append_matched_binary_result, append_right_fill_result,
    default_left_fill_labels,
};
use apply_binary_sample_value::apply_binary_sample_value;
use apply_histogram_float_binary::apply_histogram_float_binary;
use apply_histogram_histogram_binary::apply_histogram_histogram_binary;
use binary_match_key::{binary_match_key, set_binary_match_key};
use binary_op::BinaryOp;
pub(super) use combine_instant_binary::combine_instant_binary;
use copy_group_labels::copy_group_labels;
use eval_many_to_one_vector_binary::eval_many_to_one_vector_binary;
use eval_one_to_many_vector_binary::eval_one_to_many_vector_binary;
use eval_one_to_one_vector_binary::eval_one_to_one_vector_binary;
use eval_vector_set_binary::eval_vector_set_binary;
use fill_missing_right::fill_missing_right;
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
use one_to_one_binary_result_labels::one_to_one_binary_result_labels;

pub(crate) fn binary_returns_bool(modifier: Option<&BinModifier>) -> bool {
    modifier.is_some_and(|modifier| modifier.return_bool)
}

pub(crate) fn apply_binary_fill_value(
    present: &InstantSample,
    fill_value: f64,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
    missing_side: MissingSide,
) -> Result<Option<SampleValue>> {
    let filled = InstantSample {
        labels: Labels::new(),
        ts_ms: present.ts_ms,
        value: SampleValue::Float(fill_value),
        drop_name: false,
    };
    match missing_side {
        MissingSide::Left => apply_binary_sample_value(&filled, present, op, modifier),
        MissingSide::Right => apply_binary_sample_value(present, &filled, op, modifier),
    }
}

pub(crate) fn eval_vector_vector_binary(
    operands: VectorOperands,
    matching: VectorMatching<'_>,
) -> Result<Vec<InstantSample>> {
    let modifier = matching.modifier;
    let card = modifier.map_or(VectorMatchCardinality::OneToOne, |modifier| {
        modifier.card.clone()
    });
    match card {
        VectorMatchCardinality::OneToOne => eval_one_to_one_vector_binary(operands, matching),
        VectorMatchCardinality::ManyToOne(group_labels) => {
            eval_many_to_one_vector_binary(operands, matching, &group_labels.labels)
        }
        VectorMatchCardinality::OneToMany(group_labels) => {
            let VectorOperands { left, right } = operands;
            eval_one_to_many_vector_binary(left, right, matching.op, modifier, &group_labels.labels)
        }
        VectorMatchCardinality::ManyToMany => Err(PromqlError::Unsupported(
            "many-to-many vector matching is only valid for set operators".to_string(),
        )),
    }
}

/// Indexes `samples` by their vector-matching key, and rejects a key that two
/// samples share with the message `duplicate_message` builds from that key.
pub(crate) fn index_by_match_key(
    samples: Vec<InstantSample>,
    modifier: Option<&BinModifier>,
    duplicate_message: impl Fn(&str) -> String,
) -> Result<BTreeMap<String, InstantSample>> {
    let mut by_key: BTreeMap<String, InstantSample> = BTreeMap::new();
    for sample in samples {
        let key = binary_match_key(&sample.labels, modifier);
        if by_key.insert(key.clone(), sample).is_some() {
            return Err(PromqlError::Exec(duplicate_message(&key)));
        }
    }
    Ok(by_key)
}

/// Rejects a modifier that only a set operator may carry.
///
/// Unreachable by construction, and so a permanent mutation survivor:
/// `promql-parser` sets `ManyToMany` only for `and`/`or`/`unless`, and
/// `combine_instant_binary` routes those away before calling this. It stays as
/// the guard for that invariant, not as live validation.
pub(crate) fn validate_binary_modifier(modifier: Option<&BinModifier>) -> Result<()> {
    let Some(modifier) = modifier else {
        return Ok(());
    };
    if matches!(modifier.card, VectorMatchCardinality::ManyToMany) {
        return Err(PromqlError::Unsupported(
            "many-to-many vector matching is only valid for set operators".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_set_modifier(modifier: Option<&BinModifier>) -> Result<()> {
    let Some(modifier) = modifier else {
        return Ok(());
    };
    if modifier.fill_values.lhs.is_some() || modifier.fill_values.rhs.is_some() {
        return Err(PromqlError::Plan(
            "fill modifiers are invalid for set operators".to_string(),
        ));
    }
    Ok(())
}
