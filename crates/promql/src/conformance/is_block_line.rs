use super::Line;

pub(crate) fn is_block_line(line: Line<'_>) -> bool {
    line.raw.starts_with(' ') || line.raw.starts_with('\t')
}
