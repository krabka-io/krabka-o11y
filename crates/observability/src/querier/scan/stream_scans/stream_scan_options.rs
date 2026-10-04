use super::{
    BTreeMap, Labels, LokiDirection, LokiStreamEncoding, LokiStreamEntry, NonZeroUsize,
    count_stream_map_lines, default_block_fetch_concurrency,
};

#[derive(Clone, Copy)]
pub(crate) struct StreamScanOptions {
    pub(crate) direction: LokiDirection,
    pub(crate) limit: Option<usize>,
    pub(crate) end_exclusive: Option<i64>,
    pub(crate) allow_limit_short_circuit: bool,
    pub(crate) block_fetch_concurrency: NonZeroUsize,
    pub(crate) encoding: LokiStreamEncoding,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{apply_loki_stream_options, loki_streams_response};

    fn streams() -> BTreeMap<Labels, Vec<LokiStreamEntry>> {
        [("a", vec![1, 5, 8, 8]), ("b", vec![3, 6, 9])]
            .into_iter()
            .map(|(app, timestamps)| {
                let labels = Labels::from([("app".into(), app.into())]);
                let entries = timestamps
                    .into_iter()
                    .enumerate()
                    .map(|(index, ts)| {
                        LokiStreamEntry::new(
                            ts,
                            format!("{app}-{index}"),
                            Labels::new(),
                            Labels::new(),
                        )
                    })
                    .collect();
                (labels, entries)
            })
            .collect()
    }

    #[test]
    fn typed_limit_matches_the_full_json_response() {
        for direction in [LokiDirection::Forward, LokiDirection::Backward] {
            for (limit, end) in [
                (Some(3), None),
                (Some(3), Some(7)),
                (Some(0), None),
                (Some(10), Some(7)),
                (None, None),
            ] {
                let options = StreamScanOptions::from_stream_options(direction, limit, None, end);
                let full = streams();
                let expected = apply_loki_stream_options(
                    loki_streams_response(full.clone(), options.encoding),
                    direction,
                    limit,
                    None,
                    end,
                );
                let mut bounded = full;
                options.trim_before_encoding(&mut bounded);
                let actual = apply_loki_stream_options(
                    loki_streams_response(bounded, options.encoding),
                    direction,
                    limit,
                    None,
                    end,
                );
                assert2::check!(actual == expected);
            }
        }
    }

    #[test]
    fn intervals_and_categorized_labels_keep_every_row_for_later_transforms() {
        let options =
            StreamScanOptions::from_stream_options(LokiDirection::Backward, Some(3), None, None);
        for options in [
            options.with_encoding(LokiStreamEncoding::CategorizeLabels),
            StreamScanOptions::from_stream_options(LokiDirection::Backward, Some(3), Some(2), None),
        ] {
            let mut bounded = streams();
            options.trim_before_encoding(&mut bounded);
            assert2::check!(bounded == streams());
        }
    }
}

impl StreamScanOptions {
    pub(crate) fn exhaustive() -> Self {
        Self {
            direction: LokiDirection::Forward,
            limit: None,
            end_exclusive: None,
            allow_limit_short_circuit: false,
            block_fetch_concurrency: default_block_fetch_concurrency(),
            encoding: LokiStreamEncoding::Folded,
        }
    }

    pub(crate) fn from_stream_options(
        direction: LokiDirection,
        limit: Option<usize>,
        interval: Option<i64>,
        end_exclusive: Option<i64>,
    ) -> Self {
        Self {
            direction,
            limit,
            end_exclusive,
            allow_limit_short_circuit: limit.is_some() && interval.is_none(),
            block_fetch_concurrency: default_block_fetch_concurrency(),
            encoding: LokiStreamEncoding::Folded,
        }
    }

    pub(crate) fn with_block_fetch_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.block_fetch_concurrency = concurrency;
        self
    }

    pub(crate) fn with_encoding(mut self, encoding: LokiStreamEncoding) -> Self {
        self.encoding = encoding;
        self
    }

    pub(crate) fn reached_limit(self, streams: &BTreeMap<Labels, Vec<LokiStreamEntry>>) -> bool {
        self.allow_limit_short_circuit
            && self
                .limit
                .is_some_and(|limit| count_stream_map_lines(streams, self.end_exclusive) >= limit)
    }

    /// Spend the folded response's limit before allocating its JSON tree.
    /// Values must already be sorted and distinct stages already applied.
    pub(crate) fn trim_before_encoding(self, streams: &mut BTreeMap<Labels, Vec<LokiStreamEntry>>) {
        // Intervals select rows later, and categorized labels regroup streams.
        // Those paths still need every row until their response transforms run.
        if !self.allow_limit_short_circuit || self.encoding != LokiStreamEncoding::Folded {
            return;
        }
        let Some(mut remaining) = self.limit else {
            return;
        };
        streams.retain(|_, entries| {
            if let Some(end) = self.end_exclusive {
                entries.retain(|entry| entry.parsed_timestamp_ns().is_none_or(|ts| ts < end));
            }
            match self.direction {
                LokiDirection::Forward => entries.truncate(remaining),
                LokiDirection::Backward => {
                    entries.drain(..entries.len().saturating_sub(remaining));
                }
            }
            remaining -= entries.len();
            !entries.is_empty()
        });
    }

    pub(crate) fn block_fetch_concurrency(self) -> usize {
        if !self.allow_limit_short_circuit {
            return self.block_fetch_concurrency.get();
        }
        self.limit
            .map_or(self.block_fetch_concurrency.get(), |limit| {
                self.block_fetch_concurrency.get().min(limit.max(1))
            })
    }
}
