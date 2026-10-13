use super::{
    Arc, BTreeSet, HashMap, LabelMatcher, ProfileError, ProfileIdentity, ProfileScan, ProfileStats,
    ProfileStore, SampleRow, SampleValues, StacktraceIdentity, SymbolDb, compile_matchers,
    encode_rows, fingerprint_labels, queried_series_fingerprint, row_matches,
};

#[derive(Clone, Copy)]
struct ProfileTimeBounds {
    oldest_ms: i64,
    newest_ms: i64,
}

#[derive(Clone, Debug, Default)]
pub struct SampleAssociations {
    pub span_id: Option<u64>,
    pub trace_id: Option<Vec<u8>>,
}

impl SampleAssociations {
    #[must_use]
    pub const fn new(span_id: Option<u64>, trace_id: Option<Vec<u8>>) -> Self {
        Self { span_id, trace_id }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SampleProvenance {
    pub span_id: Option<u64>,
    pub trace_id: Option<Vec<u8>>,
    pub wal_sample_ids: Vec<Vec<u8>>,
}

impl SampleProvenance {
    #[must_use]
    pub fn new(
        span_id: Option<u64>,
        trace_id: Option<Vec<u8>>,
        wal_sample_ids: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            span_id,
            trace_id,
            wal_sample_ids,
        }
    }
}

/// In-memory `ProfileStore` used by engine tests.
#[derive(Clone, Debug, Default)]
pub struct InMemoryProfileStore {
    pub(crate) samples: HashMap<String, Vec<SampleRow>>,
    pub(crate) symbols: SymbolDb,
}

impl InMemoryProfileStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            samples: HashMap::new(),
            symbols: SymbolDb::new(),
        }
    }

    pub fn symbols_mut(&mut self) -> &mut SymbolDb {
        &mut self.symbols
    }

    pub fn push_sample(
        &mut self,
        profile: ProfileIdentity<'_>,
        labels: Vec<crate::ProfileLabel>,
        stack: StacktraceIdentity,
        value: i64,
        timestamp_ms: i64,
    ) {
        self.push_sample_with_total(
            profile,
            labels,
            stack,
            SampleValues::new(value, value),
            timestamp_ms,
        );
    }

    pub fn push_sample_with_total(
        &mut self,
        profile: ProfileIdentity<'_>,
        labels: Vec<crate::ProfileLabel>,
        stack: StacktraceIdentity,
        values: SampleValues,
        timestamp_ms: i64,
    ) {
        self.push_sample_with_total_and_associations(
            profile,
            labels,
            stack,
            values,
            timestamp_ms,
            SampleAssociations::default(),
        );
    }

    pub fn push_sample_with_total_and_span(
        &mut self,
        profile: ProfileIdentity<'_>,
        labels: Vec<crate::ProfileLabel>,
        stack: StacktraceIdentity,
        values: SampleValues,
        timestamp_ms: i64,
        span_id: u64,
    ) {
        self.push_sample_with_total_and_associations(
            profile,
            labels,
            stack,
            values,
            timestamp_ms,
            SampleAssociations::new(Some(span_id), None),
        );
    }

    pub fn push_sample_with_total_and_associations(
        &mut self,
        profile: ProfileIdentity<'_>,
        labels: Vec<crate::ProfileLabel>,
        stack: StacktraceIdentity,
        values: SampleValues,
        timestamp_ms: i64,
        associations: SampleAssociations,
    ) {
        self.push_sample_with_provenance(
            profile,
            labels,
            stack,
            values,
            timestamp_ms,
            SampleProvenance::new(associations.span_id, associations.trace_id, Vec::new()),
        );
    }

    /// Appends a sample with the WAL identities of its source samples.
    pub fn push_sample_with_provenance(
        &mut self,
        profile: ProfileIdentity<'_>,
        labels: Vec<crate::ProfileLabel>,
        stack: StacktraceIdentity,
        values: SampleValues,
        timestamp_ms: i64,
        associations: SampleProvenance,
    ) {
        let ProfileIdentity {
            tenant,
            profile_type,
        } = profile;
        let StacktraceIdentity {
            partition,
            stacktrace_id,
        } = stack;
        let SampleValues { value, total_value } = values;
        let SampleProvenance {
            span_id,
            trace_id,
            wal_sample_ids,
        } = associations;
        let fingerprint = fingerprint_labels(&labels);
        self.samples
            .entry(tenant.to_string())
            .or_default()
            .push(SampleRow {
                profile_type: profile_type.to_string(),
                fingerprint,
                labels,
                partition,
                stacktrace_id,
                value,
                total_value,
                span_id,
                trace_id,
                wal_sample_ids,
                timestamp_ms,
            });
    }
}

#[async_trait::async_trait]
impl ProfileStore for InMemoryProfileStore {
    async fn select(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileScan, ProfileError> {
        let compiled = compile_matchers(matchers)?;
        let rows: Vec<&SampleRow> = self
            .samples
            .get(tenant)
            .into_iter()
            .flat_map(|rows| rows.iter())
            .filter(|row| row.profile_type == profile_type)
            .filter(|row| row.timestamp_ms >= start_ms && row.timestamp_ms <= end_ms)
            .filter(|row| row_matches(row, &compiled))
            .collect();
        let batch = encode_rows(&rows)?;
        let table = crate::profile_samples_mem_table(vec![batch])
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        ProfileScan::from_mem_table(table, Arc::new(self.symbols.clone()))
    }

    async fn query_stats(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<crate::ProfileQueryStats, ProfileError> {
        let compiled = compile_matchers(matchers)?;
        // This store is one ingester head. Head.Series applies time bounds to
        // the whole head and then lists all selector-matching series, including
        // those with no individual profile timestamp in the requested window.
        let bounds = self.rows_in_range(tenant, i64::MIN, i64::MAX).fold(
            None::<ProfileTimeBounds>,
            |bounds, row| {
                Some(bounds.map_or(
                    ProfileTimeBounds {
                        oldest_ms: row.timestamp_ms,
                        newest_ms: row.timestamp_ms,
                    },
                    |old| ProfileTimeBounds {
                        oldest_ms: old.oldest_ms.min(row.timestamp_ms),
                        newest_ms: old.newest_ms.max(row.timestamp_ms),
                    },
                ))
            },
        );
        if !bounds.is_some_and(|bounds| start_ms <= bounds.newest_ms && end_ms >= bounds.oldest_ms)
        {
            return Ok(crate::ProfileQueryStats::default());
        }
        let mut stats = crate::ProfileQueryStats::default();
        let mut profiles = BTreeSet::new();
        let mut physical_series = BTreeSet::new();
        let rows: Vec<_> = self.rows_in_range(tenant, i64::MIN, i64::MAX).collect();
        for row in &rows {
            physical_series.insert(row.fingerprint);
            profiles.insert(crate::ProfileInstanceKey {
                fingerprint: row.fingerprint,
                profile_type: row.profile_type.clone(),
                timestamp_ms: row.timestamp_ms,
            });
            if (profile_type.is_empty() || row.profile_type == profile_type)
                && row_matches(row, &compiled)
            {
                stats
                    .fingerprints
                    .insert(queried_series_fingerprint(&row.labels));
            }
        }
        stats.profile_count = profiles.len() as u64;
        stats.profiles = profiles;
        stats.sample_count = rows.len() as u64;
        stats.block_count = u64::from(!rows.is_empty());
        // Measure the retained row, label and association buffers, rather than
        // a newly materialized query batch. Allocator metadata and HashMap
        // bucket overhead are excluded from these backend payload counts.
        let retained = self.samples.get(tenant).map_or(&[][..], Vec::as_slice);
        let mut profile_bytes = self
            .samples
            .get(tenant)
            .map_or(0, |rows| rows.capacity() * std::mem::size_of::<SampleRow>());
        let mut index_bytes = 0;
        for row in retained {
            profile_bytes += row.profile_type.capacity();
            profile_bytes += row.trace_id.as_ref().map_or(0, Vec::capacity);
            profile_bytes += row.wal_sample_ids.capacity() * std::mem::size_of::<Vec<u8>>();
            profile_bytes += row.wal_sample_ids.iter().map(Vec::capacity).sum::<usize>();
            index_bytes += row.labels.capacity() * std::mem::size_of::<crate::ProfileLabel>();
            index_bytes += row
                .labels
                .iter()
                .map(|label| label.name.capacity() + label.value.capacity())
                .sum::<usize>();
        }
        stats.scopes.push(crate::ProfileQueryScope {
            component_type: "Short term storage",
            component_count: u64::from(stats.block_count > 0),
            block_count: stats.block_count,
            series_count: physical_series.len() as u64,
            profile_count: stats.profile_count,
            sample_count: stats.sample_count,
            index_bytes: index_bytes as u64,
            profile_bytes: profile_bytes as u64,
            symbol_bytes: self.symbols.payload_memory_bytes() as u64,
        });
        Ok(stats)
    }

    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        let compiled = compile_matchers(matchers)?;
        let mut names = BTreeSet::new();
        for row in self.rows_in_range(tenant, start_ms, end_ms) {
            if !row_matches(row, &compiled) {
                continue;
            }
            names.extend(row.labels.iter().map(|label| label.name.clone()));
        }
        Ok(names.into_iter().collect())
    }

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        let compiled = compile_matchers(matchers)?;
        let mut values = BTreeSet::new();
        for row in self.rows_in_range(tenant, start_ms, end_ms) {
            if !row_matches(row, &compiled) {
                continue;
            }
            for label in &row.labels {
                if label.name == name {
                    values.insert(label.value.clone());
                }
            }
        }
        Ok(values.into_iter().collect())
    }

    async fn profile_types(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        let mut types = BTreeSet::new();
        for row in self.rows_in_range(tenant, start_ms, end_ms) {
            types.insert(row.profile_type.clone());
        }
        Ok(types.into_iter().collect())
    }

    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
        start_ms: i64,
        end_ms: i64,
    ) -> crate::ProfileSeriesResult {
        let compiled = compile_matchers(matchers)?;
        let mut out = BTreeSet::new();
        for row in self.rows_in_range(tenant, start_ms, end_ms) {
            if !row_matches(row, &compiled) {
                continue;
            }
            // An empty `label_names` means "return the full label set" (the
            // Pyroscope `/series` convention). Projecting onto an empty name
            // list yields an empty vec, which surfaces as a spurious `[{}]`
            // entry — mirror the cold-path fix in `krabka_blockstore`'s index.
            let mut projected: Vec<_> = if label_names.is_empty() {
                row.labels.iter().cloned().collect()
            } else {
                label_names
                    .iter()
                    .filter_map(|want| row.labels.iter().find(|label| label.name == *want).cloned())
                    .collect()
            };
            // Pyroscope's `/series` emits each set's labels SORTED by name, in
            // both the projected and full-label-set forms (e.g. `__profile_type__`
            // before `service_name`). `row.labels` is in ingest insertion order and
            // the projection follows the request's `label_names` order, so sort
            // here to match the wire order the Grafana drilldown compares against.
            projected.sort();
            if !projected.is_empty() || label_names.is_empty() {
                out.insert(projected);
            }
        }
        Ok(out.into_iter().collect())
    }

    async fn stats(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileStats, ProfileError> {
        let mut oldest = None;
        let mut newest = None;
        for row in self.rows_in_range(tenant, start_ms, end_ms) {
            oldest =
                Some(oldest.map_or(row.timestamp_ms, |value: i64| value.min(row.timestamp_ms)));
            newest =
                Some(newest.map_or(row.timestamp_ms, |value: i64| value.max(row.timestamp_ms)));
        }
        Ok(ProfileStats {
            data_ingested: oldest.is_some(),
            oldest_profile_time: oldest,
            newest_profile_time: newest,
        })
    }
}

impl InMemoryProfileStore {
    pub(crate) fn rows_in_range(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> impl Iterator<Item = &SampleRow> {
        self.samples
            .get(tenant)
            .into_iter()
            .flat_map(|rows| rows.iter())
            .filter(move |row| row.timestamp_ms >= start_ms && row.timestamp_ms <= end_ms)
    }
}
