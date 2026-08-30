use super::*;

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
            .iter()
            .flat_map(|(tenant, series)| {
                series
                    .iter()
                    .map(|(fingerprint, labels)| ManifestSeries {
                        tenant: tenant.clone(),
                        fingerprint: *fingerprint,
                        labels: labels.clone(),
                    })
                    .collect::<Vec<_>>()
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
                series.iter().map(|(fingerprint, labels)| ManifestSeries {
                    tenant: tenant.to_string(),
                    fingerprint: *fingerprint,
                    labels: labels.clone(),
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
                    .iter()
                    .filter(|(fingerprint, _)| shard_fingerprints.contains(fingerprint))
                    .map(|(fingerprint, labels)| ManifestSeries {
                        tenant: tenant.to_string(),
                        fingerprint: *fingerprint,
                        labels: labels.clone(),
                    })
            })
            .collect();

        Self {
            format_version: LOG_INDEX_MANIFEST_VERSION,
            series,
            blocks,
        }
    }

    pub(crate) fn into_indexes(self) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
        self.into_indexes_filtered(None)
    }

    pub(crate) fn into_indexes_for_tenant(
        self,
        tenant: &str,
    ) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
        self.into_indexes_filtered(Some(tenant))
    }

    pub(crate) fn into_indexes_filtered(
        self,
        tenant: Option<&str>,
    ) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
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

        Ok((label_index, block_index))
    }
}
