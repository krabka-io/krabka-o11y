use super::*;

pub(crate) fn read_tree_varint(body: &[u8], pos: &mut usize, field: &str) -> Result<u64, ProfilesError> {
    let mut value = 0_u64;
    let mut shift = 0_u32;
    loop {
        let byte = *body
            .get(*pos)
            .ok_or_else(|| ProfilesError::Decode(format!("tree payload ended before {field}")))?;
        *pos += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err(ProfilesError::Decode(format!(
                "tree {field} varint overflows u64"
            )));
        }
    }
}
