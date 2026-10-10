use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashSet},
};

use super::{
    BTreeMap, BlockDescriptor, Labels, LokiDirection, LokiStreamEncoding, LokiStreamEntry,
    LokiStreamOptions, NonZeroUsize, default_block_fetch_concurrency,
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
        options: LokiStreamOptions,
        end_exclusive: Option<i64>,
    ) -> Self {
        let LokiStreamOptions {
            direction,
            limit,
            interval,
        } = options;
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

    /// Ordered blocks after `next_block` cannot outrank `limit` strict winners.
    /// Equal timestamps must still be read for stream and entry tie ordering.
    pub(crate) fn can_skip_remaining_blocks(
        self,
        streams: &BTreeMap<Labels, Vec<LokiStreamEntry>>,
        hot_streams: &BTreeMap<Labels, Vec<LokiStreamEntry>>,
        next_block: &BlockDescriptor,
    ) -> bool {
        if !self.allow_limit_short_circuit || self.encoding != LokiStreamEncoding::Folded {
            return false;
        }
        let Some(limit) = self.limit else {
            return false;
        };
        // A cold row can still be in the hot tail until the frontier advances.
        let mut seen = HashSet::new();
        streams
            .iter()
            .chain(hot_streams.iter())
            .flat_map(|(labels, entries)| entries.iter().map(move |entry| (labels, entry)))
            .filter(|(_, entry)| {
                entry.parsed_timestamp_ns().is_some_and(|timestamp| {
                    self.end_exclusive.is_none_or(|end| timestamp < end)
                        && match self.direction {
                            LokiDirection::Forward => {
                                timestamp < next_block.key.time_range.start_ns
                            }
                            LokiDirection::Backward => timestamp > next_block.key.time_range.end_ns,
                        }
                })
            })
            .filter(|(labels, entry)| {
                seen.insert((*labels, entry.timestamp_ns.as_str(), entry.line.as_str()))
            })
            .take(limit)
            .count()
            == limit
    }

    /// Spend the folded response's limit before allocating its JSON tree.
    /// Values must already be sorted and distinct stages already applied.
    pub(crate) fn trim_before_encoding(self, streams: &mut BTreeMap<Labels, Vec<LokiStreamEntry>>) {
        // Intervals select rows later, and categorized labels regroup streams.
        if !self.allow_limit_short_circuit || self.encoding != LokiStreamEncoding::Folded {
            return;
        }
        let Some(mut remaining) = self.limit else {
            return;
        };
        // Equal timestamps can interleave cold and hot copies of different lines.
        for entries in streams.values_mut() {
            let mut keep = {
                let mut seen = HashSet::new();
                entries
                    .iter()
                    .map(|entry| seen.insert((entry.timestamp_ns.as_str(), entry.line.as_str())))
                    .collect::<Vec<_>>()
            }
            .into_iter();
            entries.retain(|_| keep.next().expect("each entry has a retention flag"));
        }
        if streams.len() > 1 {
            self.trim_multiple_streams_before_encoding(streams, remaining);
            return;
        }
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

    fn trim_multiple_streams_before_encoding(
        self,
        streams: &mut BTreeMap<Labels, Vec<LokiStreamEntry>>,
        limit: usize,
    ) {
        if limit == 0 {
            streams.clear();
            return;
        }
        let entry_count = streams.values().fold(0_usize, |count, entries| {
            count.saturating_add(entries.len())
        });
        if entry_count <= limit {
            return;
        }
        // Production entries have integer timestamps. Keep the JSON fallback
        // for malformed entries, whose global limiter uses a different key.
        if streams
            .values()
            .flatten()
            .any(|entry| entry.parsed_timestamp_ns().is_none())
        {
            return;
        }
        if let Some(end) = self.end_exclusive {
            for entries in streams.values_mut() {
                entries.retain(|entry| entry.parsed_timestamp_ns().is_some_and(|ts| ts < end));
            }
        }
        let priority = |entry: &LokiStreamEntry| {
            let timestamp = i128::from(entry.parsed_timestamp_ns().expect("checked above"));
            match self.direction {
                LokiDirection::Forward => timestamp,
                LokiDirection::Backward => -timestamp,
            }
        };
        // These indices match the folded response, which iterates the same
        // map. Sorting serialized labels would change escaped-label ties.
        let by_stream = streams.values().collect::<Vec<_>>();
        let mut selected = vec![0; by_stream.len()];
        let mut heads = BinaryHeap::with_capacity(by_stream.len());
        for (stream_index, entries) in by_stream.iter().enumerate() {
            if entries.is_empty() {
                continue;
            }
            let entry_index = match self.direction {
                LokiDirection::Forward => 0,
                LokiDirection::Backward => entries.len() - 1,
            };
            heads.push(Reverse((
                priority(&entries[entry_index]),
                stream_index,
                entry_index,
            )));
        }
        for _ in 0..limit {
            let Some(Reverse((_, stream_index, entry_index))) = heads.pop() else {
                break;
            };
            selected[stream_index] += 1;
            let entries = by_stream[stream_index];
            let next = match self.direction {
                LokiDirection::Forward => {
                    (entry_index + 1 < entries.len()).then_some(entry_index + 1)
                }
                LokiDirection::Backward => entry_index.checked_sub(1),
            };
            if let Some(next) = next {
                heads.push(Reverse((priority(&entries[next]), stream_index, next)));
            }
        }
        let mut counts = selected.into_iter();
        streams.retain(|_, entries| {
            let count = counts.next().expect("one count per stream");
            match self.direction {
                LokiDirection::Forward => entries.truncate(count),
                LokiDirection::Backward => {
                    entries.drain(..entries.len() - count);
                }
            }
            !entries.is_empty()
        });
    }

    pub(crate) fn block_fetch_concurrency(self) -> usize {
        if !self.allow_limit_short_circuit || self.encoding != LokiStreamEncoding::Folded {
            return self.block_fetch_concurrency.get();
        }
        self.limit
            .map_or(self.block_fetch_concurrency.get(), |limit| {
                self.block_fetch_concurrency.get().min(limit.max(1))
            })
    }
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

    fn escaped_and_unicode_streams() -> BTreeMap<Labels, Vec<LokiStreamEntry>> {
        let mut streams = ["\n", "\"", "/", "\\", "é", "λ", "🎉"]
            .into_iter()
            .map(|app| {
                let labels = Labels::from([("app".into(), app.into())]);
                let entries: Vec<_> = [1, 5, 5, 7, 9]
                    .into_iter()
                    .enumerate()
                    .map(|(index, timestamp)| {
                        LokiStreamEntry::new(
                            timestamp,
                            format!("{app}-{index}"),
                            Labels::from([("source".into(), "metadata".into())]),
                            Labels::from([("parser".into(), "value".into())]),
                        )
                    })
                    .collect();
                (labels, entries)
            })
            .collect::<BTreeMap<_, _>>();
        // A map and its strict extension sort differently as label maps and
        // serialized JSON objects. Equal times expose that distinction.
        let entries = streams
            .get(&Labels::from([("app".into(), "/".into())]))
            .unwrap()
            .clone();
        streams.insert(
            Labels::from([("app".into(), "/".into()), ("zone".into(), "é".into())]),
            entries,
        );
        streams
    }

    fn extreme_streams() -> BTreeMap<Labels, Vec<LokiStreamEntry>> {
        [
            ("a", vec![i64::MIN, -1, 0, i64::MAX]),
            ("b", vec![i64::MIN, 0, i64::MAX]),
        ]
        .into_iter()
        .map(|(app, times)| {
            let entries = times
                .into_iter()
                .map(|time| {
                    LokiStreamEntry::new(
                        time,
                        format!("{app}-{time}"),
                        Labels::new(),
                        Labels::new(),
                    )
                })
                .collect();
            (Labels::from([("app".into(), app.into())]), entries)
        })
        .collect()
    }

    fn malformed_streams() -> BTreeMap<Labels, Vec<LokiStreamEntry>> {
        let mut streams = extreme_streams();
        let mut malformed =
            LokiStreamEntry::new(0, "malformed".into(), Labels::new(), Labels::new());
        malformed.timestamp_ns = "not-a-timestamp".into();
        streams.insert(Labels::from([("app".into(), "c".into())]), vec![malformed]);
        streams
    }

    #[test]
    fn typed_limit_matches_the_full_json_response() {
        for direction in [LokiDirection::Forward, LokiDirection::Backward] {
            for (limit, end) in [
                (Some(1), None),
                (Some(2), None),
                (Some(3), None),
                (Some(9), None),
                (Some(3), Some(7)),
                (Some(0), None),
                (Some(10), Some(7)),
                (None, None),
            ] {
                let stream_options = LokiStreamOptions {
                    direction,
                    limit,
                    interval: None,
                };
                let options = StreamScanOptions::from_stream_options(stream_options, end);
                for full in [
                    streams(),
                    streams().into_iter().take(1).collect::<BTreeMap<_, _>>(),
                    escaped_and_unicode_streams(),
                    extreme_streams(),
                    malformed_streams(),
                ] {
                    let expected = apply_loki_stream_options(
                        loki_streams_response(full.clone(), options.encoding),
                        stream_options,
                        end,
                    );
                    let mut bounded = full;
                    options.trim_before_encoding(&mut bounded);
                    let actual = apply_loki_stream_options(
                        loki_streams_response(bounded, options.encoding),
                        stream_options,
                        end,
                    );
                    assert2::check!(actual == expected);
                }
            }
        }
    }

    #[test]
    fn intervals_and_categorized_labels_keep_every_row_for_later_transforms() {
        let backward_three = LokiStreamOptions {
            direction: LokiDirection::Backward,
            limit: Some(3),
            interval: None,
        };
        for options in [
            StreamScanOptions::from_stream_options(backward_three, None)
                .with_encoding(LokiStreamEncoding::CategorizeLabels),
            StreamScanOptions::from_stream_options(
                LokiStreamOptions {
                    interval: Some(2),
                    ..backward_three
                },
                None,
            ),
        ] {
            let mut bounded = streams();
            options.trim_before_encoding(&mut bounded);
            assert2::check!(bounded == streams());
        }
    }

    #[test]
    fn block_fetch_concurrency_keeps_the_configured_bound_for_exhaustive_scans() {
        for (encoding, limit, interval, configured, expected) in [
            (LokiStreamEncoding::Folded, Some(1), None, 8, 1),
            (LokiStreamEncoding::CategorizeLabels, Some(1), None, 8, 8),
            (LokiStreamEncoding::Folded, Some(0), None, 8, 1),
            (LokiStreamEncoding::CategorizeLabels, Some(0), None, 8, 8),
            (LokiStreamEncoding::Folded, Some(3), None, 8, 3),
            (LokiStreamEncoding::Folded, Some(12), None, 8, 8),
            (LokiStreamEncoding::Folded, None, None, 8, 8),
            (LokiStreamEncoding::CategorizeLabels, None, None, 8, 8),
            (LokiStreamEncoding::Folded, Some(1), Some(2), 8, 8),
            (LokiStreamEncoding::CategorizeLabels, Some(1), Some(2), 8, 8),
            (LokiStreamEncoding::Folded, Some(0), Some(0), 8, 8),
            (LokiStreamEncoding::Folded, Some(1), None, 1, 1),
            (LokiStreamEncoding::CategorizeLabels, Some(1), None, 1, 1),
        ] {
            let options = StreamScanOptions::from_stream_options(
                LokiStreamOptions {
                    direction: LokiDirection::Forward,
                    limit,
                    interval,
                },
                None,
            )
            .with_encoding(encoding)
            .with_block_fetch_concurrency(NonZeroUsize::new(configured).unwrap());
            assert2::check!(options.block_fetch_concurrency() == expected);
        }

        let mut options = StreamScanOptions::from_stream_options(
            LokiStreamOptions {
                direction: LokiDirection::Forward,
                limit: Some(1),
                interval: None,
            },
            None,
        )
        .with_block_fetch_concurrency(NonZeroUsize::new(8).unwrap());
        options.allow_limit_short_circuit = false;
        assert2::check!(options.block_fetch_concurrency() == 8);
    }
}
