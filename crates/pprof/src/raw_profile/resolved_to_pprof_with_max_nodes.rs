use super::{
    BTreeMap, Frame, PprofBuilder, PprofProfile, ProfileType, ResolvedFunction, ResolvedLocation,
};

/// Mirrors the resolver's function-tree cutoff, including tied cumulative totals.
/// The virtual root has value zero; it contributes to the node count, but does
/// not accumulate samples. A marked root can therefore create an unused stub.
pub(crate) fn resolved_to_pprof_with_max_nodes(
    samples: BTreeMap<Vec<ResolvedLocation>, i64>,
    profile_type: &ProfileType,
    max_nodes: i64,
) -> PprofProfile {
    let mut totals = BTreeMap::<Vec<ResolvedFunction>, i64>::new();
    for (locations, value) in &samples {
        let mut path = Vec::new();
        for function in locations
            .iter()
            .rev()
            .flat_map(|location| location.lines.iter().rev().map(|line| &line.function))
        {
            path.push(function.clone());
            *totals.entry(path.clone()).or_default() += value;
        }
    }
    let limit = usize::try_from(max_nodes).unwrap_or_default();
    let threshold = if limit == 0 || limit >= totals.len().saturating_add(1) {
        0
    } else {
        let mut values = totals.values().copied().chain([0]).collect::<Vec<_>>();
        values.sort_unstable_by(|a, b| b.cmp(a));
        values[limit - 1]
    };
    let other = ResolvedLocation::from(Frame {
        function: "other".to_string(),
        file: String::new(),
        line: 0,
    });
    let mut merged = BTreeMap::<Vec<ResolvedLocation>, i64>::new();
    for (mut locations, value) in samples {
        let functions = locations
            .iter()
            .flat_map(|location| location.lines.iter().map(|line| line.function.clone()))
            .collect::<Vec<_>>();
        let offset = (0..functions.len()).find(|offset| {
            let path = functions[*offset..]
                .iter()
                .rev()
                .cloned()
                .collect::<Vec<_>>();
            totals.get(&path).is_some_and(|total| *total >= threshold)
        });
        match offset {
            None => locations = vec![other.clone()],
            Some(0) => {}
            Some(offset) => {
                // Upstream removes locations from the root end while walking
                // inline functions, then replaces the remaining leaf with other.
                let mut f = functions.len();
                let mut l = locations.len();
                while l > 0 && f >= offset {
                    f = f.saturating_sub(locations[l - 1].lines.len());
                    l -= 1;
                }
                locations = if l > 0 {
                    std::iter::once(other.clone())
                        .chain(locations[l..].iter().cloned())
                        .collect()
                } else {
                    locations
                };
            }
        }
        *merged.entry(locations).or_default() += value;
    }
    let mut builder = PprofBuilder::new(profile_type);
    for (locations, value) in merged {
        builder.add_resolved_sample(&locations, value);
    }
    if threshold > 0 {
        builder.location_id(&other);
    }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    fn location(name: &str) -> ResolvedLocation {
        ResolvedLocation::from(Frame {
            function: name.to_owned(),
            file: String::new(),
            line: 0,
        })
    }
    #[test]
    fn cumulative_cutoff_keeps_shared_root_and_ties() {
        let profile_type =
            ProfileType::parse("process_cpu:cpu:nanoseconds:cpu:nanoseconds").unwrap();
        let samples = BTreeMap::from([
            (vec![location("root")], 40),
            (vec![location("hot"), location("root")], 100),
        ]);
        let profile = resolved_to_pprof_with_max_nodes(samples, &profile_type, 1);
        let inner = profile.inner();
        check!(
            inner
                .sample
                .iter()
                .map(|sample| sample.value[0])
                .sum::<i64>()
                == 140
        );
        check!(inner.sample.len() == 2);
        check!(inner.location.len() == 3);
        let tied = BTreeMap::from([(vec![location("left")], 7), (vec![location("right")], 7)]);
        let profile = resolved_to_pprof_with_max_nodes(tied, &profile_type, 1);
        check!(profile.inner().sample.len() == 2);
        check!(
            profile
                .inner()
                .sample
                .iter()
                .map(|sample| sample.value[0])
                .sum::<i64>()
                == 14
        );
    }
}
