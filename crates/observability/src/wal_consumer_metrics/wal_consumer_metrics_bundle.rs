use super::{
    ConsumerRecord, Counter, Family, Gauge, Histogram, Registry, Time, TimeExt, WalPartitionLabel,
    WalPollOutcome, WalPollOutcomeLabel,
};

/// Delay buckets for one WAL record, in seconds.
///
/// The low end is a consumer that is keeping up, where the delay is the
/// producer's own batching plus one network hop. The high end is a consumer
/// that has to replay a backlog, where the oldest record it reads can be hours
/// old. The spread matches
/// `cortex_ingest_storage_reader_receive_delay_seconds`, so a Mimir panel
/// reads this histogram with the same quantile expression.
const RECEIVE_DELAY_BUCKETS: [f64; 12] = [
    0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0, 3600.0,
];

/// The WAL consumer instruments, as a cheaply-clonable bundle of handles.
///
/// Build one with [`WalConsumerMetrics::register`] inside a signal's
/// `ServiceMetrics`, then call [`Self::record_poll`] at the poll site.
#[derive(Clone, Debug)]
pub struct WalConsumerMetrics {
    records: Family<WalPartitionLabel, Counter>,
    last_consumed_offset: Family<WalPartitionLabel, Gauge>,
    polls: Family<WalPollOutcomeLabel, Counter>,
    receive_delay: Histogram,
    partition_owned: Family<WalPartitionLabel, Gauge>,
    partition_revocations: Family<WalPartitionLabel, Counter>,
}

impl WalConsumerMetrics {
    /// Registers the instruments into a `wal_consumer` sub-registry of
    /// `registry`, and returns the handles.
    ///
    /// The names inherit the service prefix, so the traces service exports
    /// `krabka_traces_wal_consumer_records_total`.
    pub fn register(registry: &mut Registry) -> Self {
        let this = Self::unregistered();
        let registry = registry.sub_registry_with_prefix("wal_consumer");

        registry.register(
            "records",
            "WAL records this role has consumed, by topic and partition.",
            this.records.clone(),
        );
        registry.register(
            "last_consumed_offset",
            "Highest WAL offset this role has consumed, by topic and \
             partition. Subtract it from the broker's \
             krabka_broker_consumer_group_lag_records to see the records read \
             but not yet committed.",
            this.last_consumed_offset.clone(),
        );
        registry.register(
            "polls",
            "WAL polls this role has made, by outcome. An `empty` poll is a \
             consumer that is caught up; a consumer whose task has stopped \
             counts no polls at all.",
            this.polls.clone(),
        );
        registry.register(
            "receive_delay_seconds",
            "Seconds between the moment a WAL record was produced and the \
             moment this role read it. One observation per partition per poll, \
             taken from the newest record in the poll.",
            this.receive_delay.clone(),
        );
        registry.register(
            "partition_owned",
            "1 while the consumer group holds this topic partition on this \
             member, 0 once the group has taken it away. Every partition of \
             the role's WAL topic should read 1 on exactly one member at all \
             times.",
            this.partition_owned.clone(),
        );
        registry.register(
            "partition_revocations",
            "Times the consumer group has taken this topic partition away \
             from this member. Any increase means records this member had \
             polled and not yet written into a block were abandoned: see \
             krabka_observability::wal_group_assignment. A group whose \
             membership never changes never increments this.",
            this.partition_revocations.clone(),
        );

        this
    }

    /// Handles that no registry holds, so nothing they record is exported.
    ///
    /// This is for a call site that has no registry, which in practice means a
    /// test. Do not use it in a service. A role that wires this instead of
    /// [`Self::register`] reads its WAL with every one of its series absent,
    /// which is the state this module exists to end.
    #[must_use]
    pub fn unregistered() -> Self {
        Self {
            records: Family::default(),
            last_consumed_offset: Family::default(),
            polls: Family::default(),
            receive_delay: Histogram::new(RECEIVE_DELAY_BUCKETS),
            partition_owned: Family::default(),
            partition_revocations: Family::default(),
        }
    }

    /// Records that the group has placed `partition` of `topic` on this member.
    ///
    /// Call it for the partitions of a first assignment as well as for the ones
    /// a later assignment adds, so the gauge states the whole ownership set
    /// rather than only its changes.
    pub fn record_partition_assigned(&self, topic: &str, partition: i32) {
        self.partition_owned
            .get_or_create(&WalPartitionLabel {
                topic: topic.to_owned(),
                partition,
            })
            .set(1);
    }

    /// Records that the group has taken `partition` of `topic` away from this
    /// member.
    ///
    /// The gauge drops to 0 and the counter moves. The counter is the one that
    /// alerts: the gauge returns to 1 on whichever member picks the partition
    /// up, so a dashboard reading only the gauge sees a group that looks
    /// healthy moments after it dropped a member's buffered records.
    pub fn record_partition_revoked(&self, topic: &str, partition: i32) {
        let label = WalPartitionLabel {
            topic: topic.to_owned(),
            partition,
        };
        self.partition_owned.get_or_create(&label).set(0);
        self.partition_revocations.get_or_create(&label).inc();
    }

    /// Records one poll that returned `records`, against the wall clock.
    ///
    /// This is the call site's entry point. [`Self::record_poll_at`] is the
    /// same recording with the clock supplied, which is what a test uses.
    ///
    /// An empty `records` is a real outcome and is counted as one. Do not skip
    /// the call: an empty poll is how a caught-up consumer proves it is still
    /// running.
    pub fn record_poll(&self, records: &[ConsumerRecord]) {
        self.record_poll_at(records, now_unix_millis());
    }

    /// Records one poll that returned `records`, at the given wall clock.
    ///
    /// `now_unix_millis` is in the same epoch milliseconds the broker stamps
    /// on a record.
    ///
    /// The cost is one pass over `records` and one map lookup per partition in
    /// the poll, not per record: the counters are reached once per partition
    /// with the whole count, and the delay is observed once per partition from
    /// the newest record. Nothing here takes a lock.
    pub fn record_poll_at(&self, records: &[ConsumerRecord], now_unix_millis: i64) {
        if records.is_empty() {
            self.record_poll_outcome(WalPollOutcome::Empty);
            return;
        }
        self.record_poll_outcome(WalPollOutcome::Records);

        // One entry per partition in the poll, which is at most the assigned
        // partition count. A linear scan beats a hash map at that size.
        let mut per_partition: Vec<PartitionTally> = Vec::new();
        for record in records {
            match per_partition
                .iter_mut()
                .find(|tally| tally.partition == record.partition && tally.topic == record.topic)
            {
                Some(tally) => tally.add(record),
                None => per_partition.push(PartitionTally::new(record)),
            }
        }

        for tally in per_partition {
            let label = WalPartitionLabel {
                topic: tally.topic.clone(),
                partition: tally.partition,
            };
            self.records.get_or_create(&label).inc_by(tally.records);
            self.last_consumed_offset
                .get_or_create(&label)
                .set(tally.max_offset);
            // Kafka spells "this record has no timestamp" as a non-positive
            // one, and the logs producer leaves it unset. Subtracting it from
            // the wall clock reports the age of the Unix epoch, which is
            // decades, and one such observation ruins every quantile drawn
            // from this histogram and fires every alert built on it. A record
            // with no produce time has no delay to report, so none is
            // reported. The record count and the offset gauge still move, and
            // those are what say the consumer is alive.
            if tally.timestamp_at_max_offset > 0 {
                // A record stamped in the future gives a negative delay, which
                // a histogram would put in the lowest bucket and report as
                // "caught up". Clamping to zero says the same thing without
                // implying the clocks agree.
                let delay = (now_unix_millis - tally.timestamp_at_max_offset).max(0);
                self.receive_delay
                    .observe(Time::from_millis(delay).secs_f64());
            }
        }
    }

    /// Records one poll that failed.
    ///
    /// Call this at the error arm of the poll, not at the error arm of the
    /// work that follows it. A poll that succeeded and a decode that failed
    /// are different faults.
    pub fn record_poll_failure(&self) {
        self.record_poll_outcome(WalPollOutcome::Error);
    }

    fn record_poll_outcome(&self, outcome: WalPollOutcome) {
        self.polls
            .get_or_create(&WalPollOutcomeLabel::from(outcome))
            .inc();
    }

    /// The consumed-record count for one topic and partition.
    #[must_use]
    pub fn records(&self, topic: &str, partition: i32) -> u64 {
        self.records
            .get_or_create(&WalPartitionLabel {
                topic: topic.to_owned(),
                partition,
            })
            .get()
    }

    /// The highest consumed offset for one topic and partition.
    #[must_use]
    pub fn last_consumed_offset(&self, topic: &str, partition: i32) -> i64 {
        self.last_consumed_offset
            .get_or_create(&WalPartitionLabel {
                topic: topic.to_owned(),
                partition,
            })
            .get()
    }

    /// Whether this member currently holds one topic partition, as 1 or 0.
    #[must_use]
    pub fn partition_owned(&self, topic: &str, partition: i32) -> i64 {
        self.partition_owned
            .get_or_create(&WalPartitionLabel {
                topic: topic.to_owned(),
                partition,
            })
            .get()
    }

    /// The revocation count for one topic and partition.
    #[must_use]
    pub fn partition_revocations(&self, topic: &str, partition: i32) -> u64 {
        self.partition_revocations
            .get_or_create(&WalPartitionLabel {
                topic: topic.to_owned(),
                partition,
            })
            .get()
    }

    /// The poll count for one outcome.
    #[must_use]
    pub fn polls(&self, outcome: WalPollOutcome) -> u64 {
        self.polls
            .get_or_create(&WalPollOutcomeLabel::from(outcome))
            .get()
    }
}

/// What one partition contributed to one poll.
struct PartitionTally {
    topic: String,
    partition: i32,
    records: u64,
    max_offset: i64,
    timestamp_at_max_offset: i64,
}

impl PartitionTally {
    fn new(record: &ConsumerRecord) -> Self {
        Self {
            topic: record.topic.clone(),
            partition: record.partition,
            records: 1,
            max_offset: record.offset,
            timestamp_at_max_offset: record.timestamp,
        }
    }

    fn add(&mut self, record: &ConsumerRecord) {
        self.records += 1;
        if record.offset > self.max_offset {
            self.max_offset = record.offset;
            self.timestamp_at_max_offset = record.timestamp;
        }
    }
}

/// The wall clock in the epoch milliseconds a Kafka record timestamp uses.
///
/// A clock set before the epoch gives zero, which reads as "no delay". It is
/// not a state a running broker produces, and the alternative is a negative
/// delay that the histogram would report as caught up.
fn now_unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
        })
}
