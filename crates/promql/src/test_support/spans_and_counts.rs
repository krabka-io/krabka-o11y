use super::*;

pub(crate) fn spans_and_counts(buckets: &[(i32, f64)]) -> (Vec<BucketSpan>, Vec<f64>) {
    let mut spans = Vec::new();
    let mut counts = Vec::new();
    let Some((first_index, first_count)) = buckets.first().copied() else {
        return (spans, counts);
    };

    let mut current_offset = first_index;
    let mut current_len = 1_u32;
    let mut previous_index = first_index;
    counts.push(first_count);

    for (index, count) in buckets.iter().copied().skip(1) {
        if index == previous_index + 1 {
            current_len += 1;
        } else {
            spans.push(BucketSpan {
                offset: current_offset,
                length: current_len,
            });
            current_offset = index - previous_index - 1;
            current_len = 1;
        }
        previous_index = index;
        counts.push(count);
    }
    spans.push(BucketSpan {
        offset: current_offset,
        length: current_len,
    });

    (spans, counts)
}
