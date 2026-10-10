use super::{BTreeMap, ProfilesError};

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
    fn stack_and_value(self, line: &str, line_number: usize) -> Result<(&str, i64), ProfilesError> {
        match self {
            Self::Lines => Ok((line, 1)),
            Self::Folded => {
                let (stack, value) = line.rsplit_once(char::is_whitespace).ok_or_else(|| {
                    ProfilesError::Decode(format!("folded line {line_number} missing value"))
                })?;
                let value = value.parse::<i64>().map_err(|err| {
                    ProfilesError::Decode(format!(
                        "folded line {line_number} has invalid value: {err}"
                    ))
                })?;
                Ok((stack, value))
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
) -> Result<BTreeMap<Vec<(String, i32)>, i64>, ProfilesError> {
    let mut stacks = BTreeMap::<Vec<(String, i32)>, i64>::new();
    for (line_no, raw_line) in body.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (stack, value) = format.stack_and_value(line, line_no + 1)?;
        let frames = stack
            .split(';')
            .filter(|frame| !frame.is_empty())
            .map(|frame| (frame.to_string(), 0))
            .collect::<Vec<_>>();
        if frames.is_empty() {
            return Err(ProfilesError::Decode(format!(
                "{} {} has empty stack",
                format.line_name(),
                line_no + 1
            )));
        }
        *stacks.entry(frames).or_default() += value;
    }

    if stacks.is_empty() {
        return Err(ProfilesError::Decode(format!(
            "{} has no samples",
            format.profile_name()
        )));
    }
    Ok(stacks)
}
