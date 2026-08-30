use super::*;

pub(crate) struct Quoted<'a>(pub(crate) &'a str);

impl fmt::Display for Quoted<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\"")?;
        for c in self.0.chars() {
            match c {
                '\\' => write!(f, "\\\\")?,
                '"' => write!(f, "\\\"")?,
                '\n' => write!(f, "\\n")?,
                '\r' => write!(f, "\\r")?,
                '\t' => write!(f, "\\t")?,
                _ => write!(f, "{c}")?,
            }
        }
        write!(f, "\"")
    }
}
