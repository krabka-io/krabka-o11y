use super::*;

type LabelPostings = BTreeMap<String, BTreeMap<String, BTreeSet<SeriesFingerprint>>>;

/// A label set in a tenant's log index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TenantSeries {
    pub fingerprint: SeriesFingerprint,
    pub labels: Labels,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LabelIndex {
    pub(crate) series: BTreeMap<String, BTreeMap<SeriesFingerprint, Labels>>,
    pub(crate) postings: BTreeMap<String, LabelPostings>,
}

impl LabelIndex {
    pub fn insert_series(
        &mut self,
        tenant: impl Into<String>,
        labels: Labels,
    ) -> SeriesFingerprint {
        let tenant = tenant.into();
        let fingerprint = series_fingerprint(&labels);
        let postings = self.postings.entry(tenant.clone()).or_default();
        for pair in labels.keys() {
            postings
                .entry(pair.clone())
                .or_default()
                .entry(labels[pair].clone())
                .or_default()
                .insert(fingerprint);
        }
        self.series
            .entry(tenant)
            .or_default()
            .insert(fingerprint, labels);
        fingerprint
    }

    #[must_use]
    pub fn match_series(
        &self,
        tenant: &str,
        predicates: &[LabelPredicate],
    ) -> BTreeSet<SeriesFingerprint> {
        let Some(series) = self.series.get(tenant) else {
            return BTreeSet::new();
        };
        let Some(candidates) = self.exact_candidates(tenant, predicates) else {
            return BTreeSet::new();
        };

        // Exact postings already decide equalities. An absent negative posting
        // excludes nothing, including series whose label is absent. These
        // selectors need neither a per-series label check nor a rebuilt set.
        if predicates.iter().all(|predicate| match predicate.op {
            MatchOp::Equal => true,
            MatchOp::NotEqual => self
                .postings
                .get(tenant)
                .and_then(|names| names.get(&predicate.name))
                .and_then(|values| values.get(&predicate.value))
                .is_none(),
            MatchOp::RegexEqual | MatchOp::RegexNotEqual => false,
        }) {
            return candidates;
        }

        candidates
            .into_iter()
            .filter(|fingerprint| {
                series.get(fingerprint).is_some_and(|labels| {
                    predicates
                        .iter()
                        .filter(|predicate| predicate.op != MatchOp::Equal)
                        .all(|predicate| predicate.matches(labels))
                })
            })
            .collect()
    }

    #[must_use]
    pub fn label_names(&self, tenant: &str) -> BTreeSet<String> {
        self.postings
            .get(tenant)
            .map_or_else(BTreeSet::new, |names| names.keys().cloned().collect())
    }

    #[must_use]
    pub fn label_values(&self, tenant: &str, label_name: &str) -> BTreeSet<String> {
        self.postings
            .get(tenant)
            .and_then(|names| names.get(label_name))
            .map_or_else(BTreeSet::new, |values| values.keys().cloned().collect())
    }

    #[must_use]
    pub fn labels_for(&self, tenant: &str, fingerprint: SeriesFingerprint) -> Option<&Labels> {
        self.series.get(tenant)?.get(&fingerprint)
    }

    #[must_use]
    pub fn tenant_series(&self, tenant: &str) -> Vec<TenantSeries> {
        self.series
            .get(tenant)
            .map(|series| {
                series
                    .keys()
                    .map(|fingerprint| TenantSeries {
                        fingerprint: *fingerprint,
                        labels: series[fingerprint].clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn exact_candidates(
        &self,
        tenant: &str,
        predicates: &[LabelPredicate],
    ) -> Option<BTreeSet<SeriesFingerprint>> {
        let mut first: Option<&BTreeSet<SeriesFingerprint>> = None;
        let mut postings = Vec::new();
        for predicate in predicates {
            let Some(key) = predicate.exact_posting_key() else {
                continue;
            };
            let posting = self.postings.get(tenant)?.get(key.name)?.get(key.value)?;
            if let Some(first) = first {
                // A single equality does not allocate a posting vector.
                if postings.is_empty() {
                    postings.push(first);
                }
                postings.push(posting);
            } else {
                first = Some(posting);
            }
        }

        let Some(first) = first else {
            return Some(
                self.series
                    .get(tenant)
                    .map_or_else(BTreeSet::new, |series| series.keys().copied().collect()),
            );
        };
        if postings.is_empty() {
            return Some(first.clone());
        }
        let mut smallest = 0;
        for position in 1..postings.len() {
            if postings[position].len() < postings[smallest].len() {
                smallest = position;
            }
        }
        let mut matched = postings.swap_remove(smallest).clone();
        for posting in postings {
            matched = matched.intersection(posting).copied().collect();
        }
        Some(matched)
    }
}
