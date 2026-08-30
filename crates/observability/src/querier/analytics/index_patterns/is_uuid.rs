/// Canonical `8-4-4-4-12` hex UUID, regardless of leading character.
pub(crate) fn is_uuid(value: &str) -> bool {
    let mut groups = value.split('-');
    let shaped = [8usize, 4, 4, 4, 12].into_iter().all(|len| {
        groups
            .next()
            .is_some_and(|group| group.len() == len && group.bytes().all(|b| b.is_ascii_hexdigit()))
    });
    shaped && groups.next().is_none()
}
