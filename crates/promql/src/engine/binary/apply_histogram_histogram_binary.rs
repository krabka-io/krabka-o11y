use super::{BinModifier, BinaryOp, NativeHistogram, ResetHint, Result, SampleValue, add_compatible_native_histogram, binary_returns_bool, emit_info, incompatible_types_in_binop_info, scale_native_histogram_values};

pub(crate) fn apply_histogram_histogram_binary(
    left: &NativeHistogram,
    right: &NativeHistogram,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
) -> Result<Option<SampleValue>> {
    let mut out = left.clone();
    match op {
        BinaryOp::Add => add_compatible_native_histogram(&mut out, right)?,
        BinaryOp::Sub => {
            let mut right = right.clone();
            scale_native_histogram_values(&mut right, -1.0);
            add_compatible_native_histogram(&mut out, &right)?;
            out.reset_hint = ResetHint::Gauge;
        }
        BinaryOp::Eq | BinaryOp::Neq => {
            let pass = match op {
                BinaryOp::Eq => left == right,
                BinaryOp::Neq => left != right,
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
        BinaryOp::Gt | BinaryOp::Lt | BinaryOp::Gte | BinaryOp::Lte => {
            // Ordered comparisons are undefined between two histograms:
            // Prometheus drops the pair and raises an info annotation.
            emit_info(incompatible_types_in_binop_info(
                "histogram",
                op.symbol(),
                "histogram",
            ));
            return Ok(None);
        }
        _ => return Ok(None),
    }
    Ok(Some(SampleValue::Histogram(out)))
}
