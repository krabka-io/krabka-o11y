use super::Line;

pub(crate) fn is_block_line(line: Line<'_>) -> bool {
    (line.raw.starts_with(' ') || line.raw.starts_with('\t'))
        && ![
            "load ",
            "load_with_nhcb ",
            "eval instant ",
            "eval range ",
            "eval_fail instant ",
            "eval_fail range ",
        ]
        .iter()
        .any(|prefix| line.trimmed.starts_with(prefix))
        && line.trimmed != "clear"
}
