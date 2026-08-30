#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricScalarArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Power,
}
