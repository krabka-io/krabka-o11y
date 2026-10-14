use super::{Result, Value, ValueArithmetic, apply_value_arithmetic};

pub(crate) fn value_mul(lhs: Value, rhs: Value) -> Result<Value> {
    apply_value_arithmetic(lhs, rhs, ValueArithmetic::Multiply)
}
