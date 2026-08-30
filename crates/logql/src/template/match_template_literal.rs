use super::advance_template_pos;

pub(crate) fn match_template_literal(value: &str, pos: &mut usize, expected: char) -> Option<()> {
    let ch = value.get(*pos..)?.chars().next()?;
    if ch != expected {
        return None;
    }
    advance_template_pos(pos, ch.len_utf8())?;
    Some(())
}
