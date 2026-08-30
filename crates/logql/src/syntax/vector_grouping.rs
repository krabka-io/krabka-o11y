
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VectorGrouping {
    By(Vec<String>),
    Without(Vec<String>),
}
