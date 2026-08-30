pub(crate) fn bucket_index(offset: i128, span: i128, buckets: usize) -> usize {
    let raw = offset * i128::try_from(buckets).expect("bucket count fits i128") / span;
    let clamped = raw.clamp(
        0,
        i128::try_from(buckets - 1).expect("bucket count fits i128"),
    );
    usize::try_from(clamped).expect("bucket index fits usize")
}
