use super::{Cow, GO_SHAPE_PREFIX};

pub(crate) fn drop_go_type_parameters(input: &str) -> Cow<'_, str> {
    if !input.contains(GO_SHAPE_PREFIX) {
        return Cow::Borrowed(input);
    }

    let mut result = String::with_capacity(input.len());
    let mut cursor = 0;
    while let Some(relative_start) = input[cursor..].find(GO_SHAPE_PREFIX) {
        let start = cursor + relative_start;
        result.push_str(&input[cursor..start]);

        let mut depth = 0_i32;
        let mut end = None;
        for (offset, byte) in input[start..].bytes().enumerate() {
            match byte {
                b'[' => depth += 1,
                b']' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(start + offset + 1);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(next) = end else {
            result.push_str(&input[start..]);
            return Cow::Owned(result);
        };
        cursor = next;
    }
    result.push_str(&input[cursor..]);
    Cow::Owned(result)
}
