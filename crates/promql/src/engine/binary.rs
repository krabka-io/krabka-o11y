use std::collections::{BTreeMap, BTreeSet};

use krabka_blockstore::Labels;
use krabka_metrics::{NativeHistogram, ResetHint};
use promql_parser::parser::{
    BinModifier, BinaryExpr, LabelModifier, VectorMatchCardinality,
    token::{
        T_ADD, T_ATAN2, T_DIV, T_EQLC, T_GTE, T_GTR, T_LAND, T_LOR, T_LSS, T_LTE, T_LUNLESS, T_MOD,
        T_MUL, T_NEQ, T_POW, T_SUB, TokenType,
    },
};

use super::{
    annotations::{emit_info, incompatible_types_in_binop_info},
    histogram::{
        add_compatible_native_histogram, scale_native_histogram_values, scaled_native_histogram,
    },
    labels::{
        float_sample_value, is_result_metadata_label, labels_key, labels_without_metric_name,
    },
};
use crate::{
    PromqlError,
    error::Result,
    result::{InstantSample, QueryResult, SampleValue},
};

// === split-modules: generated submodules ===
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
mod instant_value;
mod missing_side;
mod one_to_one_binary_result_labels;
mod scalar_side;
mod set_op;
mod validate_binary_modifier;
mod validate_set_modifier;

use apply_binary_fill_value::apply_binary_fill_value;
use apply_binary_sample_value::apply_binary_sample_value;
use apply_histogram_float_binary::apply_histogram_float_binary;
use apply_histogram_histogram_binary::apply_histogram_histogram_binary;
use binary_match_key::binary_match_key;
use binary_op::BinaryOp;
use binary_returns_bool::binary_returns_bool;
pub(super) use combine_instant_binary::combine_instant_binary;
use copy_group_labels::copy_group_labels;
use eval_many_to_one_vector_binary::eval_many_to_one_vector_binary;
use eval_one_to_many_vector_binary::eval_one_to_many_vector_binary;
use eval_one_to_one_vector_binary::eval_one_to_one_vector_binary;
use eval_vector_set_binary::eval_vector_set_binary;
use eval_vector_vector_binary::eval_vector_vector_binary;
pub(super) use instant_value::InstantValue;
use missing_side::MissingSide;
use one_to_one_binary_result_labels::one_to_one_binary_result_labels;
use scalar_side::ScalarSide;
use set_op::SetOp;
use validate_binary_modifier::validate_binary_modifier;
use validate_set_modifier::validate_set_modifier;
