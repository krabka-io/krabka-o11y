use super::{InstantSample, PromqlError, Regex, Result};
use crate::PromqlString;

pub(crate) fn apply_byte_label_replace(
    samples: Vec<InstantSample>,
    dst: &PromqlString,
    replacement: &PromqlString,
    src: &PromqlString,
    pattern: &PromqlString,
) -> Result<Vec<InstantSample>> {
    let pattern = pattern.utf8().ok_or_else(|| {
        PromqlError::Exec("invalid regular expression in label_replace()".to_owned())
    })?;
    // Go's regex decoder maps invalid bytes to RuneError, but capture indexes
    // still address the original bytes. Match its rune view, then map captures
    // back to their original byte ranges before expanding the replacement.
    let regex = Regex::new(&format!("^(?s:{pattern})$"))
        .map_err(|error| PromqlError::Exec(format!("invalid label_replace regex: {error}")))?;
    let dst = dst.utf8().filter(|name| !name.is_empty()).ok_or_else(|| {
        PromqlError::Exec("invalid destination label name in label_replace()".to_owned())
    })?;
    let capture_name = Regex::new(r"^[_\p{L}\p{Nd}]+").expect("static capture-name expression");
    Ok(samples
        .into_iter()
        .map(|mut sample| {
            let empty = PromqlString::default();
            let source = src
                .utf8()
                .and_then(|name| sample.labels.get_value(name))
                .unwrap_or(&empty);
            if let Some(captures) = regex.captures(source.as_str()) {
                let mut value = Vec::new();
                let replacement = replacement.as_bytes();
                let mut index = 0;
                while index < replacement.len() {
                    if replacement[index] != b'$' {
                        value.push(replacement[index]);
                        index += 1;
                        continue;
                    }
                    if replacement.get(index + 1) == Some(&b'$') {
                        value.push(b'$');
                        index += 2;
                        continue;
                    }
                    let wrapped = replacement.get(index + 1) == Some(&b'{');
                    let start = index + if wrapped { 2 } else { 1 };
                    let suffix = &replacement[start..];
                    let valid = match std::str::from_utf8(suffix) {
                        Ok(valid) => valid,
                        Err(error) => std::str::from_utf8(&suffix[..error.valid_up_to()])
                            .expect("valid UTF-8 prefix"),
                    };
                    let end = start + capture_name.find(valid).map_or(0, |name| name.end());
                    if start == end || (wrapped && replacement.get(end) != Some(&b'}')) {
                        value.push(b'$');
                        index += 1;
                        continue;
                    }
                    let name = std::str::from_utf8(&replacement[start..end])
                        .expect("validated capture name");
                    let capture = if name.bytes().all(|byte| byte.is_ascii_digit())
                        && (name == "0" || !name.starts_with('0'))
                    {
                        name.parse::<usize>()
                            .ok()
                            .and_then(|index| captures.get(index))
                    } else {
                        captures.name(name)
                    };
                    if let Some(capture) = capture {
                        value.extend_from_slice(
                            source
                                .bytes_for_json_range(capture.start(), capture.end())
                                .expect("regex captures end on decoded rune boundaries"),
                        );
                    }
                    index = end + usize::from(wrapped);
                }
                sample.labels =
                    super::set_label_value(&sample.labels, dst, crate::PromqlString::from(value));
                if dst == "__name__" {
                    sample.drop_name = false;
                }
            }
            sample
        })
        .collect())
}
