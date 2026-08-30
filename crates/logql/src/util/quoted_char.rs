use super::*;

pub(crate) struct QuotedChar(pub(crate) char);

impl fmt::Display for QuotedChar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "'{}'", self.0)
    }
}
