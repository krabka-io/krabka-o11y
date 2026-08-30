use super::Read;

pub(crate) fn read_capped_reader(mut reader: impl Read, cap: u64) -> Option<Vec<u8>> {
    let cap_usize = usize::try_from(cap).unwrap_or(usize::MAX);
    let mut buf = Vec::new();
    let mut chunk = vec![0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut chunk).ok()?;
        if read == 0 {
            break;
        }
        if buf.len().saturating_add(read) > cap_usize {
            return None;
        }
        buf.extend_from_slice(&chunk[..read]);
    }
    Some(buf)
}
