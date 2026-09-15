use super::{BTreeMap, BTreeSet, PprofBuilder, PprofProfile, ProfileType, ResolvedLocation};

pub(crate) fn resolved_to_pprof_with_max_nodes(
    samples: BTreeMap<Vec<ResolvedLocation>, i64>,
    profile_type: &ProfileType,
    max_nodes: i64,
) -> PprofProfile {
    let limit = usize::try_from(max_nodes.max(1)).unwrap_or(usize::MAX);
    let locations = samples
        .keys()
        .flat_map(|stack| stack.iter().cloned())
        .collect::<BTreeSet<_>>();
    let mut builder = PprofBuilder::new(profile_type);
    // Pyroscope counts the tree's virtual root toward max_nodes.
    if locations.len().saturating_add(1) <= limit {
        for (stack, value) in samples {
            builder.add_resolved_sample(&stack, value);
        }
        return builder.finish();
    }

    let mut ranked = samples.into_iter().collect::<Vec<_>>();
    ranked.sort_by(|(left_stack, left_value), (right_stack, right_value)| {
        right_value
            .unsigned_abs()
            .cmp(&left_value.unsigned_abs())
            .then_with(|| left_stack.cmp(right_stack))
    });
    let mut used = BTreeSet::new();
    let mut kept = Vec::new();
    let mut omitted = 0_i64;
    let budget = limit.saturating_sub(1);
    for (stack, value) in ranked {
        let additions = stack
            .iter()
            .filter(|location| !used.contains(*location))
            .count();
        if used.len().saturating_add(additions) <= budget {
            used.extend(stack.iter().cloned());
            kept.push((stack, value));
        } else {
            omitted = omitted.saturating_add(value);
        }
    }
    for (stack, value) in kept {
        builder.add_resolved_sample(&stack, value);
    }
    if omitted != 0 {
        builder.add_sample(&["other".to_string()], omitted);
    }
    builder.finish()
}
