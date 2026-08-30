use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetricVectorGroupModifier {
    Left(Vec<String>),
    Right(Vec<String>),
}
