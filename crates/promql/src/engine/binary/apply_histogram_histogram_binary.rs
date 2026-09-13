use std::cmp::Ordering;

use super::{
    BinModifier, BinaryOp, NativeHistogram, ResetHint, Result, SampleValue,
    add_compatible_native_histogram, binary_returns_bool, emit_info,
    incompatible_types_in_binop_info, mismatched_custom_buckets_info,
    scale_native_histogram_values,
};

pub(crate) fn apply_histogram_histogram_binary(
    left: &NativeHistogram,
    right: &NativeHistogram,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
) -> Result<Option<SampleValue>> {
    let mut out = left.clone();
    match op {
        BinaryOp::Add => {
            note_custom_reconciliation(left, right, "addition");
            add_compatible_native_histogram(&mut out, right)?;
        }
        BinaryOp::Sub => {
            note_custom_reconciliation(left, right, "subtraction");
            let mut right = right.clone();
            scale_native_histogram_values(&mut right, -1.0);
            add_compatible_native_histogram(&mut out, &right)?;
            out.reset_hint = ResetHint::Gauge;
        }
        BinaryOp::Eq | BinaryOp::Neq => {
            let equal = native_histograms_equal(left, right);
            let pass = match op {
                BinaryOp::Eq => equal,
                BinaryOp::Neq => !equal,
                _ => unreachable!("non-comparison histogram op"),
            };
            return Ok(if binary_returns_bool(modifier) {
                Some(SampleValue::Float(if pass { 1.0 } else { 0.0 }))
            } else if pass {
                Some(SampleValue::Histogram(left.clone()))
            } else {
                None
            });
        }
        // Every other operator is undefined between two histograms -- the
        // ordered comparisons, and `*`, `/`, `%`, `^` and `atan2` alike.
        // Prometheus drops the pair and raises an info annotation.
        _ => {
            emit_info(incompatible_types_in_binop_info(
                "histogram",
                op.symbol(),
                "histogram",
            ));
            return Ok(None);
        }
    }
    Ok(Some(SampleValue::Histogram(out)))
}

fn note_custom_reconciliation(left: &NativeHistogram, right: &NativeHistogram, operation: &str) {
    if left.is_nhcb() && right.is_nhcb() && left.custom_values != right.custom_values {
        emit_info(mismatched_custom_buckets_info(operation));
    }
}

fn native_histograms_equal(left: &NativeHistogram, right: &NativeHistogram) -> bool {
    left.schema == right.schema
        && left.count.to_bits() == right.count.to_bits()
        && left.sum.to_bits() == right.sum.to_bits()
        && left.zero_threshold.partial_cmp(&right.zero_threshold) == Some(Ordering::Equal)
        && left.zero_count.to_bits() == right.zero_count.to_bits()
        && left.custom_values == right.custom_values
        && left.positive_spans == right.positive_spans
        && left.negative_spans == right.negative_spans
        && counts_equal(&left.positive_counts, &right.positive_counts)
        && counts_equal(&left.negative_counts, &right.negative_counts)
}

fn counts_equal(left: &[f64], right: &[f64]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.to_bits() == right.to_bits())
}
