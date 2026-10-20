use super::{BTreeMap, LegacyStackFrame, ProfilesError};

struct ParsedTextStackLine<'a> {
    stack: &'a str,
    value: i64,
}

/// A plain-text stack format that the legacy ingest endpoint accepts.
#[derive(Clone, Copy)]
pub(crate) enum TextStackFormat {
    /// `frame;frame;frame value` per line, as Brendan Gregg's collapse
    /// scripts write it.
    Folded,
    /// `frame;frame;frame` per line, each line one sample.
    Lines,
}

impl TextStackFormat {
    /// How an error names one line of a body in this format.
    fn line_name(self) -> &'static str {
        match self {
            Self::Folded => "folded line",
            Self::Lines => "lines profile line",
        }
    }

    /// How an error names a whole body in this format.
    fn profile_name(self) -> &'static str {
        match self {
            Self::Folded => "folded profile",
            Self::Lines => "lines profile",
        }
    }

    /// Splits `line`, the `line_number`th of its body, into its stack and the
    /// sample value it adds to that stack.
    fn stack_and_value(
        self,
        line: &str,
        line_number: usize,
    ) -> Result<ParsedTextStackLine<'_>, ProfilesError> {
        match self {
            Self::Lines => Ok(ParsedTextStackLine {
                stack: line,
                value: 1,
            }),
            Self::Folded => {
                let stack_and_value = line.rsplit_once(char::is_whitespace).ok_or_else(|| {
                    ProfilesError::Decode(format!("folded line {line_number} missing value"))
                })?;
                let value = stack_and_value.1.parse::<i64>().map_err(|err| {
                    ProfilesError::Decode(format!(
                        "folded line {line_number} has invalid value: {err}"
                    ))
                })?;
                Ok(ParsedTextStackLine {
                    stack: stack_and_value.0,
                    value,
                })
            }
        }
    }
}

/// Sums the samples of a text-stack body per stack, root frame first.
///
/// Blank lines and `#` comments are skipped.
///
/// # Errors
/// Returns a decode error for a line whose stack is empty or whose value is
/// missing or invalid, and for a body that holds no samples.
pub(crate) fn parse_text_stacks(
    body: &str,
    format: TextStackFormat,
) -> Result<BTreeMap<Vec<LegacyStackFrame>, i64>, ProfilesError> {
    let mut stacks = BTreeMap::<Vec<LegacyStackFrame>, i64>::new();
    for line_entry in body.lines().enumerate() {
        let line_no = line_entry.0;
        let line = line_entry.1.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parsed = format.stack_and_value(line, line_no + 1)?;
        let frames = super::legacy_stack_frames(parsed.stack);
        if frames.is_empty() {
            return Err(ProfilesError::Decode(format!(
                "{} {} has empty stack",
                format.line_name(),
                line_no + 1
            )));
        }
        *stacks.entry(frames).or_default() += parsed.value;
    }

    if stacks.is_empty() {
        return Err(ProfilesError::Decode(format!(
            "{} has no samples",
            format.profile_name()
        )));
    }
    Ok(stacks)
}
