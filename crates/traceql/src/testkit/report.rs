use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Report {
    pub cases: Vec<CaseResult>,
}

impl Report {
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub fn write_to(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, self.to_text())
    }

    #[must_use]
    pub fn to_text(&self) -> String {
        let passed = self.cases.iter().filter(|case| case.passed).count();
        let total = self.cases.len();
        let mut out = format!("TraceQL conformance: {passed}/{total} cases passed\n");
        for case in &self.cases {
            let status = if case.passed { "PASS" } else { "FAIL" };
            let _ = writeln!(
                out,
                "{status} {} ({}/{}) {}",
                case.name, case.passed_assertions, case.total_assertions, case.message
            );
        }
        out
    }
}
