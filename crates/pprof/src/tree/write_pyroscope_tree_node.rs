use super::write_uvarint;

pub(crate) fn write_pyroscope_tree_node(out: &mut Vec<u8>, name: &str, self_: i64) {
    write_uvarint(out, name.len() as u64);
    out.extend_from_slice(name.as_bytes());
    write_uvarint(out, self_.cast_unsigned());
}
