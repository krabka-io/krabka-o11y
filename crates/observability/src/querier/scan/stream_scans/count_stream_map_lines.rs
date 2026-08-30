use super::*;

pub(crate) fn count_stream_map_lines(
    streams: &BTreeMap<Labels, Vec<[String; 2]>>,
    end_exclusive: Option<i64>,
) -> usize {
    streams
        .values()
        .map(|values| {
            values
                .iter()
                .filter(|entry| {
                    end_exclusive.is_none_or(|end_exclusive| {
                        entry[0]
                            .parse::<i64>()
                            .map_or(true, |timestamp| timestamp < end_exclusive)
                    })
                })
                .count()
        })
        .fold(0_usize, usize::saturating_add)
}
