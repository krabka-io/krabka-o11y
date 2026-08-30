use super::*;

/// Buffer holding polled hot-tail records.
///
/// Records arrive from Kafka polling in NO timestamp order, so the buffer keeps
/// two views of the same data:
///
/// * `records` is the append-ordered log. [`records`](Self::records) clones
///   this whole list. It backs the WebSocket tail path, which indexes into the
///   buffer by arrival order and must see every record.
/// * `buckets` is a per-minute time index. It maps each
///   [`hot_tail_bucket_key`] to the positions in `records` whose timestamps
///   land in that bucket. An out-of-order arrival lands in the correct bucket,
///   and no global sort is ever needed.
///
/// [`records_in_range`](Self::records_in_range) walks only the buckets that
/// overlap the query window, so a 30-minute query over a buffer that holds
/// hours of logs touches only the window's records, instead of a scan of the
/// entire buffer.
#[derive(Debug)]
pub(crate) struct HotTailBuffer {
    pub(crate) bucket_width: Time,
    pub(crate) records: Vec<WalLogRecord>,
    pub(crate) buckets: BTreeMap<i64, Vec<usize>>,
}

impl HotTailBuffer {
    pub(crate) fn push(&mut self, record: WalLogRecord) {
        let index = self.records.len();
        let bucket = hot_tail_bucket_key(record.timestamp_ns, self.bucket_width);
        self.records.push(record);
        self.buckets.entry(bucket).or_default().push(index);
    }

    pub(crate) fn prune_compacted(&mut self, frontier: &CompactionFrontier) -> usize {
        let before = self.records.len();
        if before == 0 {
            return 0;
        }

        let old_records = std::mem::take(&mut self.records);
        self.records = old_records
            .into_iter()
            .filter(|record| !frontier.is_compacted(record))
            .collect();
        let pruned = before - self.records.len();
        // `> 0` is a permanent survivor against `>= 0`: rebuilding the bucket
        // index from an unchanged record list produces the index already held,
        // and shrinking spare capacity is not observable.
        if pruned > 0 {
            self.records.shrink_to_fit();
            self.rebuild_buckets();
        }
        pruned
    }

    pub(crate) fn rebuild_buckets(&mut self) {
        self.buckets.clear();
        for (index, record) in self.records.iter().enumerate() {
            self.buckets
                .entry(hot_tail_bucket_key(record.timestamp_ns, self.bucket_width))
                .or_default()
                .push(index);
        }
        for indices in self.buckets.values_mut() {
            indices.shrink_to_fit();
        }
    }

    pub(crate) fn records_in_range(&self, start_ns: i64, end_ns: i64) -> Vec<WalLogRecord> {
        if start_ns > end_ns {
            return Vec::new();
        }
        let start_bucket = hot_tail_bucket_key(start_ns, self.bucket_width);
        let end_bucket = hot_tail_bucket_key(end_ns, self.bucket_width);
        let mut matches: Vec<usize> = Vec::new();
        for (_bucket, indices) in self.buckets.range(start_bucket..=end_bucket) {
            for &index in indices {
                let record = &self.records[index];
                if record.timestamp_ns >= start_ns && record.timestamp_ns <= end_ns {
                    matches.push(index);
                }
            }
        }
        // Restore append order so the windowed slice matches a full-buffer scan
        // exactly (downstream collects into a BTreeMap and re-sorts, so order is not
        // load-bearing, but matching the full-scan order keeps the two paths trivially
        // equivalent for testing and reasoning).
        matches.sort_unstable();
        matches
            .into_iter()
            .map(|index| self.records[index].clone())
            .collect()
    }
}

impl Default for HotTailBuffer {
    fn default() -> Self {
        Self {
            bucket_width: minutes(1),
            records: Vec::new(),
            buckets: BTreeMap::new(),
        }
    }
}
