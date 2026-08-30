use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LabelFormatValue {
    Rename(String),
    Template(LineFormat),
}
