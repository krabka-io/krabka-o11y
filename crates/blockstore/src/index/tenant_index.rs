use super::{
    BTreeMap, BTreeSet, BlockEntry, BlockStoreError, Deserialize, LabelMatcher, Labels, MatchOp,
    QUERY_SHARD_LABEL, Result, Serialize, SeriesFingerprint, anchored_regex,
    parse_query_shard_selector,
};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct TenantIndex {
    pub(crate) series: BTreeMap<SeriesFingerprint, Labels>,
    /// `name -> value -> fingerprints`. The map is structured, and not an
    /// in-band `name\0value` key, so arbitrary label bytes, NUL included, can
    /// never collide distinct `(name, value)` pairs into one bucket.
    pub(crate) postings: BTreeMap<String, BTreeMap<String, BTreeSet<SeriesFingerprint>>>,
    pub(crate) values: BTreeMap<String, BTreeSet<String>>,
    pub(crate) blocks: Vec<BlockEntry>,
}

impl TenantIndex {
    pub(crate) fn all_fingerprints(&self) -> BTreeSet<SeriesFingerprint> {
        self.series.keys().copied().collect()
    }

    pub(crate) fn exact_posting(&self, name: &str, value: &str) -> BTreeSet<SeriesFingerprint> {
        self.postings
            .get(name)
            .and_then(|values| values.get(value))
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn resolve_one(
        &self,
        label_matcher: &LabelMatcher,
    ) -> Result<BTreeSet<SeriesFingerprint>> {
        if label_matcher.name == QUERY_SHARD_LABEL {
            return self.resolve_query_shard(label_matcher);
        }

        match label_matcher.op {
            MatchOp::Eq => {
                if label_matcher.value.is_empty() {
                    let present = self.present_fingerprints(&label_matcher.name);
                    let mut matched: BTreeSet<SeriesFingerprint> = self
                        .series
                        .keys()
                        .copied()
                        .filter(|fp| !present.contains(fp))
                        .collect();
                    matched.extend(self.exact_posting(&label_matcher.name, ""));
                    Ok(matched)
                } else {
                    Ok(self.exact_posting(&label_matcher.name, &label_matcher.value))
                }
            }
            MatchOp::Neq => {
                let excluded = if label_matcher.value.is_empty() {
                    let present = self.present_fingerprints(&label_matcher.name);
                    let mut excluded: BTreeSet<SeriesFingerprint> = self
                        .series
                        .keys()
                        .copied()
                        .filter(|fp| !present.contains(fp))
                        .collect();
                    excluded.extend(self.exact_posting(&label_matcher.name, ""));
                    excluded
                } else {
                    self.exact_posting(&label_matcher.name, &label_matcher.value)
                };
                Ok(self
                    .series
                    .keys()
                    .copied()
                    .filter(|fp| !excluded.contains(fp))
                    .collect())
            }
            MatchOp::Re | MatchOp::Nre => self.resolve_regex(label_matcher),
        }
    }

    pub(crate) fn resolve_query_shard(
        &self,
        label_matcher: &LabelMatcher,
    ) -> Result<BTreeSet<SeriesFingerprint>> {
        let selector = parse_query_shard_selector(&label_matcher.value).map_err(|error| {
            BlockStoreError::InvalidBlock(format!("invalid query shard matcher: {error}"))
        })?;
        match label_matcher.op {
            MatchOp::Eq => Ok(self
                .series
                .keys()
                .copied()
                .filter(|fp| selector.matches(*fp))
                .collect()),
            MatchOp::Neq => Ok(self
                .series
                .keys()
                .copied()
                .filter(|fp| !selector.matches(*fp))
                .collect()),
            MatchOp::Re | MatchOp::Nre => Err(BlockStoreError::InvalidBlock(
                "query shard matcher must use equality or inequality".to_string(),
            )),
        }
    }

    pub(crate) fn resolve_regex(
        &self,
        label_matcher: &LabelMatcher,
    ) -> Result<BTreeSet<SeriesFingerprint>> {
        let regex = regex::Regex::new(&anchored_regex(&label_matcher.value)).map_err(|error| {
            BlockStoreError::InvalidBlock(format!("invalid label matcher regex: {error}"))
        })?;

        let mut matched_fps = BTreeSet::new();
        if regex.is_match("") {
            let present = self.present_fingerprints(&label_matcher.name);
            matched_fps.extend(
                self.series
                    .keys()
                    .copied()
                    .filter(|fp| !present.contains(fp)),
            );
        }
        if let Some(values) = self.postings.get(&label_matcher.name) {
            for (value, fps) in values {
                if regex.is_match(value) {
                    matched_fps.extend(fps.iter().copied());
                }
            }
        }

        if label_matcher.op == MatchOp::Re {
            Ok(matched_fps)
        } else {
            Ok(self
                .all_fingerprints()
                .difference(&matched_fps)
                .copied()
                .collect())
        }
    }

    pub(crate) fn present_fingerprints(&self, name: &str) -> BTreeSet<SeriesFingerprint> {
        self.postings
            .get(name)
            .map(|values| {
                values
                    .values()
                    .flat_map(|fps| fps.iter().copied())
                    .collect()
            })
            .unwrap_or_default()
    }
}
