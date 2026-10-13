use super::*;

/// The in-memory label and block indexes decoded from one log manifest.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LogIndexes {
    pub labels: LabelIndex,
    pub blocks: BlockIndex,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LogIndexManifest {
    pub(crate) format_version: u32,
    pub(crate) series: Vec<ManifestSeries>,
    pub(crate) blocks: Vec<BlockDescriptor>,
}

impl LogIndexManifest {
    pub(crate) fn from_indexes(label_index: &LabelIndex, block_index: &BlockIndex) -> Self {
        let series = label_index
            .series
            .keys()
            .flat_map(|tenant| {
                let series = &label_index.series[tenant];
                series.keys().map(|fingerprint| ManifestSeries {
                    tenant: tenant.clone(),
                    fingerprint: *fingerprint,
                    labels: series[fingerprint].clone(),
                })
            })
            .collect();

        Self {
            format_version: LOG_INDEX_MANIFEST_VERSION,
            series,
            blocks: block_index.blocks.clone(),
        }
    }

    pub(crate) fn from_indexes_for_tenant(
        tenant: &str,
        label_index: &LabelIndex,
        block_index: &BlockIndex,
    ) -> Self {
        let series = label_index
            .series
            .get(tenant)
            .into_iter()
            .flat_map(|series| {
                series.keys().map(|fingerprint| ManifestSeries {
                    tenant: tenant.to_string(),
                    fingerprint: *fingerprint,
                    labels: series[fingerprint].clone(),
                })
            })
            .collect();
        let blocks = block_index
            .blocks
            .iter()
            .filter(|block| block.key.tenant == tenant)
            .cloned()
            .collect();

        Self {
            format_version: LOG_INDEX_MANIFEST_VERSION,
            series,
            blocks,
        }
    }

    pub(crate) fn from_indexes_for_tenant_shard(
        tenant: &str,
        shard_range: TimeRange,
        label_index: &LabelIndex,
        block_index: &BlockIndex,
    ) -> Self {
        let blocks = block_index
            .blocks
            .iter()
            .filter(|block| {
                block.key.tenant == tenant && block.key.time_range.overlaps(shard_range)
            })
            .cloned()
            .collect::<Vec<_>>();
        let shard_fingerprints = blocks
            .iter()
            .flat_map(|block| block.fingerprints.iter().copied())
            .collect::<BTreeSet<_>>();
        let series = label_index
            .series
            .get(tenant)
            .into_iter()
            .flat_map(|series| {
                series
                    .keys()
                    .filter(|fingerprint| shard_fingerprints.contains(*fingerprint))
                    .map(|fingerprint| ManifestSeries {
                        tenant: tenant.to_string(),
                        fingerprint: *fingerprint,
                        labels: series[fingerprint].clone(),
                    })
            })
            .collect();

        Self {
            format_version: LOG_INDEX_MANIFEST_VERSION,
            series,
            blocks,
        }
    }

    pub(crate) fn into_indexes(self) -> Result<LogIndexes, BlockStoreError> {
        self.into_indexes_filtered(None)
    }

    pub(crate) fn into_indexes_for_tenant(
        self,
        tenant: &str,
    ) -> Result<LogIndexes, BlockStoreError> {
        self.into_indexes_filtered(Some(tenant))
    }

    pub(crate) fn into_indexes_filtered(
        self,
        tenant: Option<&str>,
    ) -> Result<LogIndexes, BlockStoreError> {
        if self.format_version != LOG_INDEX_MANIFEST_VERSION {
            return Err(BlockStoreError::InvalidManifestVersion {
                actual: self.format_version,
                expected: LOG_INDEX_MANIFEST_VERSION,
            });
        }

        let mut label_index = LabelIndex::default();
        for series in self
            .series
            .into_iter()
            .filter(|series| tenant.is_none_or(|tenant| series.tenant == tenant))
        {
            let actual = label_index.insert_series(series.tenant, series.labels);
            if actual != series.fingerprint {
                return Err(BlockStoreError::ManifestFingerprintMismatch {
                    expected: series.fingerprint,
                    actual,
                });
            }
        }

        let mut block_index = BlockIndex::default();
        for block in self
            .blocks
            .into_iter()
            .filter(|block| tenant.is_none_or(|tenant| block.key.tenant == tenant))
        {
            block_index.insert(block);
        }

        Ok(LogIndexes {
            labels: label_index,
            blocks: block_index,
        })
    }
}
