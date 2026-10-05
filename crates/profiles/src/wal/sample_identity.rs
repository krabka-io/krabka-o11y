pub(crate) fn sample_identity(partition: i32, offset: i64, ordinal: u64) -> Vec<u8> {
    [
        partition.to_be_bytes().as_slice(),
        offset.to_be_bytes().as_slice(),
        ordinal.to_be_bytes().as_slice(),
    ]
    .concat()
}
