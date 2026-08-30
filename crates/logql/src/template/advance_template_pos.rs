
pub(crate) fn advance_template_pos(pos: &mut usize, amount: usize) -> Option<()> {
    *pos = pos.checked_add(amount)?;
    Some(())
}
