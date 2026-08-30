use super::{
    Arc, BTreeSet, HashMap, LabelMatcher, MemTable, ProfileError, ProfileScan, ProfileStats,
    ProfileStore, SampleRow, SessionContext, SymbolDb, compile_matchers, encode_rows,
    fingerprint_labels, profile_samples_schema, row_matches,
};

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
        profile: (&str, &str),
        labels: Vec<(String, String)>,
        stack: (u64, u32),
        value: i64,
        timestamp_ms: i64,
    ) {
        self.push_sample_with_total(profile, labels, stack, (value, value), timestamp_ms);
    }

    pub fn push_sample_with_total(
        &mut self,
        profile: (&str, &str),
        labels: Vec<(String, String)>,
        stack: (u64, u32),
        values: (i64, i64),
        timestamp_ms: i64,
    ) {
        self.push_sample_with_total_and_associations(
            profile,
            labels,
            stack,
            values,
            timestamp_ms,
            (None, None),
        );
    }

    pub fn push_sample_with_total_and_span(
        &mut self,
        profile: (&str, &str),
        labels: Vec<(String, String)>,
        stack: (u64, u32),
        values: (i64, i64),
        timestamp_ms: i64,
        span_id: u64,
    ) {
        self.push_sample_with_total_and_associations(
            profile,
            labels,
            stack,
            values,
            timestamp_ms,
            (Some(span_id), None),
        );
    }

    pub fn push_sample_with_total_and_associations(
        &mut self,
        profile: (&str, &str),
        labels: Vec<(String, String)>,
        stack: (u64, u32),
        values: (i64, i64),
        timestamp_ms: i64,
        associations: (Option<u64>, Option<Vec<u8>>),
    ) {
        let (tenant, profile_type) = profile;
        let (partition, stacktrace_id) = stack;
        let (value, total_value) = values;
        let (span_id, trace_id) = associations;
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
        let table = MemTable::try_new(profile_samples_schema(), vec![vec![batch]])
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        let ctx = SessionContext::new();
        let samples_table = "samples".to_string();
        ctx.register_table(&samples_table, Arc::new(table))
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        Ok(ProfileScan {
            ctx,
            samples_table,
            symbols: Arc::new(self.symbols.clone()),
        })
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
            names.extend(row.labels.iter().map(|(name, _)| name.clone()));
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
            for (label_name, value) in &row.labels {
                if label_name == name {
                    values.insert(value.clone());
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
    ) -> Result<Vec<Vec<(String, String)>>, ProfileError> {
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
                row.labels
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect()
            } else {
                label_names
                    .iter()
                    .filter_map(|want| {
                        row.labels
                            .iter()
                            .find(|(name, _)| name == want)
                            .map(|(name, value)| (name.clone(), value.clone()))
                    })
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
