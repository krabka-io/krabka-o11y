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

#[derive(Hash, PartialEq, Eq)]
struct StreamEntryIdentity<'a, Timestamp> {
    labels: &'a Labels,
    timestamp_ns: &'a Timestamp,
    line: &'a str,
}

#[derive(Hash, PartialEq, Eq)]
struct LocalEntryIdentity<'a, Timestamp> {
    timestamp_ns: &'a Timestamp,
    line: &'a str,
}

struct StreamEntryRef<'a> {
    labels: &'a Labels,
    entry: &'a LokiStreamEntry,
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
struct StreamHeapEntry {
    priority: i128,
    stream_index: usize,
    entry_index: usize,
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
        [streams, hot_streams]
            .into_iter()
            .flat_map(|stream_map| stream_map.keys().zip(stream_map.values()))
            .flat_map(|stream| {
                stream.1.iter().map(move |entry| StreamEntryRef {
                    labels: stream.0,
                    entry,
                })
            })
            .filter(|candidate| {
                candidate
                    .entry
                    .parsed_timestamp_ns()
                    .is_some_and(|timestamp| {
                        self.end_exclusive.is_none_or(|end| timestamp < end)
                            && match self.direction {
                                LokiDirection::Forward => {
                                    timestamp < next_block.key.time_range.start_ns
                                }
                                LokiDirection::Backward => {
                                    timestamp > next_block.key.time_range.end_ns
                                }
                            }
                    })
            })
            .filter(|candidate| {
                seen.insert(StreamEntryIdentity {
                    labels: candidate.labels,
                    timestamp_ns: &candidate.entry.timestamp_ns,
                    line: candidate.entry.line.as_str(),
                })
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
                    .map(|entry| {
                        seen.insert(LocalEntryIdentity {
                            timestamp_ns: &entry.timestamp_ns,
                            line: entry.line.as_str(),
                        })
                    })
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
        for stream_index in 0..by_stream.len() {
            let entries = by_stream[stream_index];
            if entries.is_empty() {
                continue;
            }
            let entry_index = match self.direction {
                LokiDirection::Forward => 0,
                LokiDirection::Backward => entries.len() - 1,
            };
            heads.push(Reverse(StreamHeapEntry {
                priority: priority(&entries[entry_index]),
                stream_index,
                entry_index,
            }));
        }
        for _ in 0..limit {
            let Some(Reverse(candidate)) = heads.pop() else {
                break;
            };
            let stream_index = candidate.stream_index;
            let entry_index = candidate.entry_index;
            selected[stream_index] += 1;
            let entries = by_stream[stream_index];
            let next = match self.direction {
                LokiDirection::Forward => {
                    (entry_index + 1 < entries.len()).then_some(entry_index + 1)
                }
                LokiDirection::Backward => entry_index.checked_sub(1),
            };
            if let Some(next) = next {
                heads.push(Reverse(StreamHeapEntry {
                    priority: priority(&entries[next]),
                    stream_index,
                    entry_index: next,
                }));
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

    struct TestLabel {
        name: &'static str,
        value: &'static str,
    }

    fn test_labels(entries: impl IntoIterator<Item = TestLabel>) -> Labels {
        let mut labels = Labels::new();
        for entry in entries {
            labels.insert(entry.name.into(), entry.value.into());
        }
        labels
    }

    struct TestStream {
        app: &'static str,
        timestamps: Vec<i64>,
    }

    struct TestEntry {
        app: &'static str,
        timestamp_ns: i64,
    }

    struct LimitCase {
        limit: Option<usize>,
        end: Option<i64>,
    }

    struct ConcurrencyCase {
        encoding: LokiStreamEncoding,
        limit: Option<usize>,
        interval: Option<i64>,
        configured: usize,
        expected: usize,
    }

    fn streams() -> BTreeMap<Labels, Vec<LokiStreamEntry>> {
        let source_streams = [
            TestStream {
                app: "a",
                timestamps: vec![1, 5, 8, 8],
            },
            TestStream {
                app: "b",
                timestamps: vec![3, 6, 9],
            },
        ];
        let mut streams = BTreeMap::new();
        for stream in source_streams {
            let labels = test_labels([TestLabel {
                name: "app",
                value: stream.app,
            }]);
            let mut index = 0;
            let entries = stream
                .timestamps
                .into_iter()
                .map(|timestamp_ns| {
                    let entry_index = index;
                    index += 1;
                    LokiStreamEntry::new(
                        timestamp_ns,
                        format!("{}-{entry_index}", stream.app),
                        Labels::new(),
                        Labels::new(),
                    )
                })
                .collect();
            streams.insert(labels, entries);
        }
        streams
    }

    fn escaped_and_unicode_streams() -> BTreeMap<Labels, Vec<LokiStreamEntry>> {
        let mut streams = BTreeMap::new();
        for app in ["\n", "\"", "/", "\\", "é", "λ", "🎉"] {
            let labels = test_labels([TestLabel {
                name: "app",
                value: app,
            }]);
            let mut index = 0;
            let entries: Vec<_> = [1, 5, 5, 7, 9]
                .into_iter()
                .map(|timestamp| {
                    let entry_index = index;
                    index += 1;
                    LokiStreamEntry::new(
                        timestamp,
                        format!("{app}-{entry_index}"),
                        test_labels([TestLabel {
                            name: "source",
                            value: "metadata",
                        }]),
                        test_labels([TestLabel {
                            name: "parser",
                            value: "value",
                        }]),
                    )
                })
                .collect();
            streams.insert(labels, entries);
        }
        // A map and its strict extension sort differently as label maps and
        // serialized JSON objects. Equal times expose that distinction.
        let entries = streams
            .get(&test_labels([TestLabel {
                name: "app",
                value: "/",
            }]))
            .unwrap()
            .clone();
        streams.insert(
            test_labels([
                TestLabel {
                    name: "app",
                    value: "/",
                },
                TestLabel {
                    name: "zone",
                    value: "é",
                },
            ]),
            entries,
        );
        streams
    }

    fn extreme_streams() -> BTreeMap<Labels, Vec<LokiStreamEntry>> {
        let source_entries = [
            TestEntry {
                app: "a",
                timestamp_ns: i64::MIN,
            },
            TestEntry {
                app: "a",
                timestamp_ns: -1,
            },
            TestEntry {
                app: "a",
                timestamp_ns: 0,
            },
            TestEntry {
                app: "a",
                timestamp_ns: i64::MAX,
            },
            TestEntry {
                app: "b",
                timestamp_ns: i64::MIN,
            },
            TestEntry {
                app: "b",
                timestamp_ns: 0,
            },
            TestEntry {
                app: "b",
                timestamp_ns: i64::MAX,
            },
        ];
        let mut streams = BTreeMap::new();
        for source_entry in source_entries {
            let labels = test_labels([TestLabel {
                name: "app",
                value: source_entry.app,
            }]);
            streams.entry(labels).or_insert_with(Vec::new).push({
                LokiStreamEntry::new(
                    source_entry.timestamp_ns,
                    format!("{}-{}", source_entry.app, source_entry.timestamp_ns),
                    Labels::new(),
                    Labels::new(),
                )
            });
        }
        streams
    }

    fn malformed_streams() -> BTreeMap<Labels, Vec<LokiStreamEntry>> {
        let mut streams = extreme_streams();
        let mut malformed =
            LokiStreamEntry::new(0, "malformed".into(), Labels::new(), Labels::new());
        malformed.timestamp_ns = "not-a-timestamp".into();
        streams.insert(
            test_labels([TestLabel {
                name: "app",
                value: "c",
            }]),
            vec![malformed],
        );
        streams
    }

    #[test]
    fn typed_limit_matches_the_full_json_response() {
        for direction in [LokiDirection::Forward, LokiDirection::Backward] {
            for case in [
                LimitCase {
                    limit: Some(1),
                    end: None,
                },
                LimitCase {
                    limit: Some(2),
                    end: None,
                },
                LimitCase {
                    limit: Some(3),
                    end: None,
                },
                LimitCase {
                    limit: Some(9),
                    end: None,
                },
                LimitCase {
                    limit: Some(3),
                    end: Some(7),
                },
                LimitCase {
                    limit: Some(0),
                    end: None,
                },
                LimitCase {
                    limit: Some(10),
                    end: Some(7),
                },
                LimitCase {
                    limit: None,
                    end: None,
                },
            ] {
                let LimitCase { limit, end } = case;
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
        for case in [
            ConcurrencyCase {
                encoding: LokiStreamEncoding::Folded,
                limit: Some(1),
                interval: None,
                configured: 8,
                expected: 1,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::CategorizeLabels,
                limit: Some(1),
                interval: None,
                configured: 8,
                expected: 8,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::Folded,
                limit: Some(0),
                interval: None,
                configured: 8,
                expected: 1,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::CategorizeLabels,
                limit: Some(0),
                interval: None,
                configured: 8,
                expected: 8,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::Folded,
                limit: Some(3),
                interval: None,
                configured: 8,
                expected: 3,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::Folded,
                limit: Some(12),
                interval: None,
                configured: 8,
                expected: 8,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::Folded,
                limit: None,
                interval: None,
                configured: 8,
                expected: 8,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::CategorizeLabels,
                limit: None,
                interval: None,
                configured: 8,
                expected: 8,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::Folded,
                limit: Some(1),
                interval: Some(2),
                configured: 8,
                expected: 8,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::CategorizeLabels,
                limit: Some(1),
                interval: Some(2),
                configured: 8,
                expected: 8,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::Folded,
                limit: Some(0),
                interval: Some(0),
                configured: 8,
                expected: 8,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::Folded,
                limit: Some(1),
                interval: None,
                configured: 1,
                expected: 1,
            },
            ConcurrencyCase {
                encoding: LokiStreamEncoding::CategorizeLabels,
                limit: Some(1),
                interval: None,
                configured: 1,
                expected: 1,
            },
        ] {
            let ConcurrencyCase {
                encoding,
                limit,
                interval,
                configured,
                expected,
            } = case;
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
