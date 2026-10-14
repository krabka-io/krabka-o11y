use super::{
    BTreeSet, LabelMatcher, MatchOp, QUERY_SHARD_LABEL, SeriesFingerprint, TenantIndex,
    anchored_regex, parse_query_shard_selector,
};

struct SelectiveSeed<'a> {
    position: usize,
    posting: Option<&'a BTreeSet<SeriesFingerprint>>,
}

impl TenantIndex {
    /// Seeds from the smallest exact posting, or a broad regex when the exact
    /// posting covers the whole tenant. `None` asks for sequential resolution.
    /// Invalid matchers also take that path, so reordering cannot change which
    /// errors an empty intermediate intersection skips.
    pub(crate) fn resolve_selective(
        &self,
        matchers: &[LabelMatcher],
    ) -> Option<BTreeSet<SeriesFingerprint>> {
        let mut seed = None;
        for (position, matcher) in matchers.iter().enumerate() {
            if matcher.op != MatchOp::Eq
                || matcher.value.is_empty()
                || matcher.name == QUERY_SHARD_LABEL
            {
                continue;
            }
            let posting = self
                .postings
                .get(&matcher.name)
                .and_then(|values| values.get(&matcher.value));
            let candidate = SelectiveSeed { position, posting };
            if seed.as_ref().is_none_or(|best: &SelectiveSeed<'_>| {
                candidate.posting.map_or(0, BTreeSet::len) < best.posting.map_or(0, BTreeSet::len)
            }) {
                seed = Some(candidate);
            }
        }
        let seed = seed?;
        let seed_position = seed.position;
        let seed_posting = seed.posting;

        // Broad regex selectors benefit from evaluating each distinct value
        // once through postings instead of looking up each candidate's labels.
        let seed_len = seed_posting.map_or(0, BTreeSet::len);
        if matchers.iter().any(|matcher| {
            matches!(matcher.op, MatchOp::Re | MatchOp::Nre)
                && self
                    .postings
                    .get(&matcher.name)
                    .is_some_and(|values| seed_len > values.len())
        }) {
            if seed_len != self.series.len() {
                return None;
            }
            return self.resolve_tenantwide_regex(matchers, seed_position);
        }

        let mut candidates = seed_posting.cloned().unwrap_or_default();
        for (position, matcher) in matchers.iter().enumerate() {
            if position != seed_position {
                self.retain_matching(&mut candidates, matcher)?;
            }
        }
        Some(candidates)
    }

    // Keep broad-regex planning out of the ordinary selective query loop.
    #[inline(never)]
    fn resolve_tenantwide_regex(
        &self,
        matchers: &[LabelMatcher],
        seed_position: usize,
    ) -> Option<BTreeSet<SeriesFingerprint>> {
        let mut regex_position = None;
        for (position, matcher) in matchers.iter().enumerate() {
            if matcher.name != QUERY_SHARD_LABEL
                && matches!(matcher.op, MatchOp::Re | MatchOp::Nre)
                && self
                    .postings
                    .get(&matcher.name)
                    .is_some_and(|values| self.series.len() > values.len())
            {
                if regex_position.is_some() {
                    return None;
                }
                regex_position = Some(position);
            }
        }
        let regex_position = regex_position?;
        if regex_position == seed_position {
            return None;
        }
        let matcher = &matchers[regex_position];
        // A tenant-wide exact posting cannot remove any regex matches.
        let mut candidates = self.resolve_regex(matcher).ok()?;
        for (position, matcher) in matchers.iter().enumerate() {
            if position != seed_position && position != regex_position {
                self.retain_matching(&mut candidates, matcher)?;
            }
        }
        Some(candidates)
    }

    fn retain_matching(
        &self,
        candidates: &mut BTreeSet<SeriesFingerprint>,
        matcher: &LabelMatcher,
    ) -> Option<()> {
        if matcher.name == QUERY_SHARD_LABEL {
            let selector = parse_query_shard_selector(&matcher.value).ok()?;
            match matcher.op {
                MatchOp::Eq => candidates.retain(|fp| selector.matches(*fp)),
                MatchOp::Neq => candidates.retain(|fp| !selector.matches(*fp)),
                MatchOp::Re | MatchOp::Nre => return None,
            }
        } else if matches!(matcher.op, MatchOp::Re | MatchOp::Nre) {
            let regex = regex::Regex::new(&anchored_regex(&matcher.value)).ok()?;
            candidates.retain(|fp| {
                let value = self
                    .series
                    .get(fp)
                    .and_then(|labels| labels.get(&matcher.name))
                    .unwrap_or_default();
                regex.is_match(value) == (matcher.op == MatchOp::Re)
            });
        } else if matcher.value.is_empty() {
            candidates.retain(|fp| {
                let value = self
                    .series
                    .get(fp)
                    .and_then(|labels| labels.get(&matcher.name))
                    .unwrap_or_default();
                value.is_empty() == (matcher.op == MatchOp::Eq)
            });
        } else {
            let posting = self
                .postings
                .get(&matcher.name)
                .and_then(|values| values.get(&matcher.value));
            candidates.retain(|fp| {
                posting.is_some_and(|posting| posting.contains(fp)) == (matcher.op == MatchOp::Eq)
            });
        }
        Some(())
    }
}
