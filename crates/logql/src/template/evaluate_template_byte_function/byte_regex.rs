use regex::Regex;

use super::runes;

pub(super) fn regex_apply(
    name: &str,
    pattern: &[u8],
    input: &[u8],
    replacement: &[u8],
) -> Option<(Vec<u8>, usize)> {
    let pattern = std::str::from_utf8(pattern).ok()?;
    let regex = Regex::new(pattern).ok()?;
    // Go's regex reader decodes each invalid byte as RuneError while retaining
    // original byte offsets for unmatched text and captures.
    let mut normalized = String::new();
    let mut offsets = vec![0];
    for (ch, start, length) in runes(input) {
        let before = normalized.len();
        normalized.push(ch);
        offsets.extend(std::iter::repeat_n(start, normalized.len() - before - 1));
        offsets.push(start + length);
    }
    let mut output = Vec::new();
    let mut end = 0;
    let mut count = 0;
    for captures in regex.captures_iter(&normalized) {
        let found = captures
            .get(0)
            .expect("regex captures include entire match");
        let start = offsets[found.start()];
        let next = offsets[found.end()];
        output.extend(&input[end..start]);
        if name == "regexReplaceAllLiteral" {
            output.extend(replacement);
        } else {
            expand(&mut output, replacement, &captures, input, &offsets);
        }
        end = next;
        count += 1;
    }
    output.extend(&input[end..]);
    Some((output, count))
}

fn expand(
    output: &mut Vec<u8>,
    replacement: &[u8],
    captures: &regex::Captures<'_>,
    input: &[u8],
    offsets: &[usize],
) {
    let mut position = 0;
    while position < replacement.len() {
        if replacement[position] != b'$' {
            output.push(replacement[position]);
            position += 1;
            continue;
        }
        position += 1;
        if replacement.get(position) == Some(&b'$') {
            output.push(b'$');
            position += 1;
            continue;
        }
        let braced = replacement.get(position) == Some(&b'{');
        let start = position + usize::from(braced);
        let mut end = start;
        while replacement
            .get(end)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        {
            end += 1;
        }
        if end == start || (braced && replacement.get(end) != Some(&b'}')) {
            output.push(b'$');
            continue;
        }
        let name = std::str::from_utf8(&replacement[start..end]).expect("ASCII replacement name");
        let found = if name.bytes().all(|byte| byte.is_ascii_digit()) {
            if name.len() > 1 && name.starts_with('0') {
                None
            } else {
                name.parse::<usize>()
                    .ok()
                    .and_then(|index| captures.get(index))
            }
        } else {
            captures.name(name)
        };
        if let Some(found) = found {
            output.extend(&input[offsets[found.start()]..offsets[found.end()]]);
        }
        position = end + usize::from(braced);
    }
}
