use super::*;

impl MetricBlockStore {
    pub(crate) fn matching_series(&self, tenant: &str, matchers: &[LabelMatcher]) -> Result<Vec<Labels>> {
        let mut by_fp = BTreeMap::<SeriesFingerprint, Labels>::new();
        for labels in self
            .floats
            .index()
            .series(tenant, matchers)
            .map_err(blockstore_error)?
        {
            by_fp.insert(labels.fingerprint(), labels);
        }
        if let Some(histograms) = &self.histograms {
            for labels in histograms
                .index()
                .series(tenant, matchers)
                .map_err(blockstore_error)?
            {
                by_fp.insert(labels.fingerprint(), labels);
            }
        }
        Ok(by_fp.into_values().collect())
    }
}

#[async_trait::async_trait]
impl MetricStore for MetricBlockStore {
    #[tracing::instrument(
        name = "promql.blockstore_scan",
        level = "debug",
        skip_all,
        fields(
            tenant = %tenant,
            matchers = matchers.len(),
            start_ms = start_ms,
            end_ms = end_ms,
            has_float = tracing::field::Empty,
            has_histograms = tracing::field::Empty
        ),
        err
    )]
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ScanResult> {
        let ctx = SessionContext::new();
        let has_float = self
            .floats
            .register_scan_table(
                &ctx,
                ScanTableRequest {
                    table_name: FLOAT_TABLE,
                    tenant,
                    matchers,
                    min_ts: start_ms,
                    max_ts: end_ms,
                    schema: float_sample_schema(),
                },
            )
            .await
            .map_err(blockstore_error)?;
        let has_histograms = if let Some(histograms) = &self.histograms {
            histograms
                .register_scan_table(
                    &ctx,
                    ScanTableRequest {
                        table_name: HISTOGRAM_TABLE,
                        tenant,
                        matchers,
                        min_ts: start_ms,
                        max_ts: end_ms,
                        schema: native_histogram_schema(),
                    },
                )
                .await
                .map_err(blockstore_error)?
        } else {
            false
        };

        let span = tracing::Span::current();
        span.record("has_float", has_float);
        span.record("has_histograms", has_histograms);

        Ok(ScanResult {
            ctx,
            float_table: has_float.then(|| FLOAT_TABLE.to_string()),
            histogram_table: has_histograms.then(|| HISTOGRAM_TABLE.to_string()),
        })
    }

    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<String>> {
        let mut names = BTreeSet::new();
        for labels in self.matching_series(tenant, matchers)? {
            names.extend(labels.iter().map(|(name, _)| name.clone()));
        }
        Ok(names.into_iter().collect())
    }

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<String>> {
        let mut values = BTreeSet::new();
        for labels in self.matching_series(tenant, matchers)? {
            if let Some(value) = labels.get(name) {
                values.insert(value.to_string());
            }
        }
        Ok(values.into_iter().collect())
    }

    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<Labels>> {
        self.matching_series(tenant, matchers)
    }

    async fn exemplars(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<ExemplarRecord>> {
        let Some(exemplars) = &self.exemplars else {
            return Ok(Vec::new());
        };
        let series_by_fp = exemplars
            .index()
            .series(tenant, matchers)
            .map_err(blockstore_error)?
            .into_iter()
            .map(|labels| (labels.fingerprint(), labels))
            .collect::<BTreeMap<_, _>>();
        if series_by_fp.is_empty() {
            return Ok(Vec::new());
        }

        let ctx = SessionContext::new();
        exemplars
            .register_scan_table(
                &ctx,
                ScanTableRequest {
                    table_name: EXEMPLAR_TABLE,
                    tenant,
                    matchers,
                    min_ts: start_ms,
                    max_ts: end_ms,
                    schema: exemplar_schema(),
                },
            )
            .await
            .map_err(blockstore_error)?;
        let batches = ctx
            .table(EXEMPLAR_TABLE)
            .await
            .map_err(datafusion_error)?
            .collect()
            .await
            .map_err(datafusion_error)?;
        let mut exemplars = Vec::new();
        for batch in batches {
            exemplars.extend(exemplars_from_batch(
                &batch,
                &series_by_fp,
                start_ms,
                end_ms,
            )?);
        }
        exemplars.sort_by_key(|row| (row.series_labels.fingerprint(), row.ts_ms));
        Ok(exemplars)
    }

    async fn metadata(&self, tenant: &str, metric: Option<&str>) -> Result<Vec<MetadataRecord>> {
        let Some(metadata) = &self.metadata else {
            return Ok(Vec::new());
        };
        let matchers = metric.map_or_else(Vec::new, |metric| {
            vec![LabelMatcher {
                name: "__name__".to_string(),
                op: krabka_blockstore::MatchOp::Eq,
                value: metric.to_string(),
            }]
        });
        if metadata
            .index()
            .series(tenant, &matchers)
            .map_err(blockstore_error)?
            .is_empty()
        {
            return Ok(Vec::new());
        }

        let ctx = SessionContext::new();
        metadata
            .register_scan_table(
                &ctx,
                ScanTableRequest {
                    table_name: METADATA_TABLE,
                    tenant,
                    matchers: &matchers,
                    min_ts: 0,
                    max_ts: i64::MAX,
                    schema: metadata_schema(),
                },
            )
            .await
            .map_err(blockstore_error)?;
        let batches = ctx
            .table(METADATA_TABLE)
            .await
            .map_err(datafusion_error)?
            .collect()
            .await
            .map_err(datafusion_error)?;
        let mut records = BTreeSet::<(String, String, String, String)>::new();
        for batch in batches {
            for record in metadata_from_batch(&batch)? {
                records.insert((
                    record.metric_family_name,
                    record.metric_type,
                    record.help,
                    record.unit,
                ));
            }
        }
        Ok(records
            .into_iter()
            .map(
                |(metric_family_name, metric_type, help, unit)| MetadataRecord {
                    metric_family_name,
                    metric_type,
                    help,
                    unit,
                },
            )
            .collect())
    }

    async fn cardinality_label_names(&self, tenant: &str) -> Result<Vec<LabelNameCardinality>> {
        let mut by_name = BTreeMap::<String, BTreeSet<SeriesFingerprint>>::new();
        for labels in self.matching_series(tenant, &[])? {
            let fp = labels.fingerprint();
            for (name, _) in labels.iter() {
                by_name.entry(name.clone()).or_default().insert(fp);
            }
        }
        let mut cardinality = by_name
            .into_iter()
            .map(|(name, fingerprints)| LabelNameCardinality {
                name,
                series_count: fingerprints.len(),
            })
            .collect::<Vec<_>>();
        cardinality.sort_by(|left, right| {
            right
                .series_count
                .cmp(&left.series_count)
                .then_with(|| left.name.cmp(&right.name))
        });
        Ok(cardinality)
    }

    async fn cardinality_label_values(&self, tenant: &str) -> Result<Vec<LabelValueCardinality>> {
        let mut by_value = BTreeMap::<(String, String), BTreeSet<SeriesFingerprint>>::new();
        for labels in self.matching_series(tenant, &[])? {
            let fp = labels.fingerprint();
            for (name, value) in labels.iter() {
                by_value
                    .entry((name.clone(), value.clone()))
                    .or_default()
                    .insert(fp);
            }
        }
        let mut cardinality = by_value
            .into_iter()
            .map(
                |((label_name, label_value), fingerprints)| LabelValueCardinality {
                    label_name,
                    label_value,
                    series_count: fingerprints.len(),
                },
            )
            .collect::<Vec<_>>();
        cardinality.sort_by(|left, right| {
            right
                .series_count
                .cmp(&left.series_count)
                .then_with(|| left.label_name.cmp(&right.label_name))
                .then_with(|| left.label_value.cmp(&right.label_value))
        });
        Ok(cardinality)
    }

    async fn cardinality_active_series(&self, tenant: &str) -> Result<Vec<Labels>> {
        self.matching_series(tenant, &[])
    }

    async fn tsdb_stats(&self, tenant: &str) -> Result<TsdbStats> {
        let series = self.matching_series(tenant, &[])?;
        let mut by_metric = BTreeMap::<String, usize>::new();
        let mut label_values_by_name = BTreeMap::<String, BTreeSet<String>>::new();
        let mut memory_by_name = BTreeMap::<String, usize>::new();
        let mut by_label_pair = BTreeMap::<String, usize>::new();
        for labels in &series {
            if let Some(metric) = labels.get("__name__") {
                *by_metric.entry(metric.to_string()).or_default() += 1;
            }
            for (name, value) in labels.iter() {
                label_values_by_name
                    .entry(name.clone())
                    .or_default()
                    .insert(value.clone());
                *memory_by_name.entry(name.clone()).or_default() += name.len() + value.len();
                *by_label_pair.entry(format!("{name}={value}")).or_default() += 1;
            }
        }

        Ok(TsdbStats {
            head_stats: TsdbHeadStats {
                num_series: series.len(),
                num_samples: 0,
                num_chunks: series.len(),
                min_time: 0,
                max_time: 0,
            },
            series_count_by_metric_name: named_stats(by_metric),
            label_value_count_by_label_name: named_stats(
                label_values_by_name
                    .into_iter()
                    .map(|(name, values)| (name, values.len()))
                    .collect(),
            ),
            memory_in_bytes_by_label_name: named_stats(memory_by_name),
            series_count_by_label_value_pair: named_stats(by_label_pair),
        })
    }

    async fn tsdb_blocks(&self, tenant: &str) -> Result<Vec<TsdbBlock>> {
        let mut blocks = self
            .floats
            .index()
            .all_blocks(tenant)
            .into_iter()
            .chain(
                self.histograms
                    .as_ref()
                    .into_iter()
                    .flat_map(|store| store.index().all_blocks(tenant)),
            )
            .map(|block| TsdbBlock {
                id: block.object_key,
                min_time: block.min_ts,
                max_time: block.max_ts,
                num_samples: block.row_count,
                num_series: block.fingerprints.len(),
            })
            .collect::<Vec<_>>();
        blocks.sort_by(|left, right| {
            left.min_time
                .cmp(&right.min_time)
                .then_with(|| left.max_time.cmp(&right.max_time))
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(blocks)
    }
}
