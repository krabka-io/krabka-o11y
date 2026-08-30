use super::*;

pub(crate) fn tree_to_pprof(
    name: &str,
    sample_unit: &str,
    body: &[u8],
    limits: LegacyDecodeLimits,
) -> Result<PprofProfile, ProfilesError> {
    let mut pos = 0;
    let mut pending = vec![Vec::<(String, i32)>::new()];
    let mut stacks = BTreeMap::<Vec<(String, i32)>, i64>::new();
    let mut node_count = 0_usize;
    let mut path_bytes = 0_usize;

    while let Some(parent_path) = pending.pop() {
        node_count += 1;
        if node_count > limits.max_nodes {
            return Err(ProfilesError::Decode(
                "tree profile exceeds node budget".to_string(),
            ));
        }

        let name_len = read_tree_varint(body, &mut pos, "node name length")?;
        let name_len = usize::try_from(name_len).map_err(|err| {
            ProfilesError::Decode(format!("tree node name length does not fit usize: {err}"))
        })?;
        let name_end = pos.checked_add(name_len).ok_or_else(|| {
            ProfilesError::Decode("tree node name length overflows payload offset".to_string())
        })?;
        if name_end > body.len() {
            return Err(ProfilesError::Decode(
                "tree node name length exceeds payload".to_string(),
            ));
        }
        let name = std::str::from_utf8(&body[pos..name_end])
            .map_err(|err| ProfilesError::Decode(format!("tree node name is not UTF-8: {err}")))?;
        pos = name_end;

        let self_value = read_tree_varint(body, &mut pos, "node self value")?;
        let children_len = read_tree_varint(body, &mut pos, "node children length")?;
        let children_len = usize::try_from(children_len).map_err(|err| {
            ProfilesError::Decode(format!(
                "tree node children length does not fit usize: {err}"
            ))
        })?;
        if children_len > body.len().saturating_sub(pos) + 1 {
            return Err(ProfilesError::Decode(
                "tree node children length exceeds remaining payload".to_string(),
            ));
        }

        let mut path = parent_path;
        if !name.is_empty() {
            path.push((name.to_string(), 0));
        }
        if self_value > 0 && !path.is_empty() {
            let value = i64::try_from(self_value).map_err(|err| {
                ProfilesError::Decode(format!("tree node self value does not fit i64: {err}"))
            })?;
            *stacks.entry(path.clone()).or_default() += value;
        }

        if children_len > 0 {
            // Each child gets its own clone of `path`; charge that copied
            // storage against the cumulative path-bytes budget so a payload
            // declaring many children of a long path cannot amplify memory
            // beyond the cap.
            let per_child_bytes = path
                .iter()
                .map(|(frame, _)| frame.len())
                .fold(0_usize, usize::saturating_add);
            let added = per_child_bytes.saturating_mul(children_len);
            path_bytes = path_bytes.saturating_add(added);
            if path_bytes > limits.max_path_bytes.bytes_usize() {
                return Err(ProfilesError::Decode(
                    "tree profile exceeds path-bytes budget".to_string(),
                ));
            }
            // Also bound the queued node count up front so an enormous declared
            // child count cannot balloon `pending` before the per-iteration
            // `node_count` check trips.
            if node_count
                .saturating_add(pending.len())
                .saturating_add(children_len)
                > limits.max_nodes
            {
                return Err(ProfilesError::Decode(
                    "tree profile exceeds node budget".to_string(),
                ));
            }
            pending.extend(std::iter::repeat_n(path, children_len));
        }
    }

    if pos != body.len() {
        return Err(ProfilesError::Decode(
            "tree profile has trailing bytes".to_string(),
        ));
    }
    if stacks.is_empty() {
        return Err(ProfilesError::Decode(
            "tree profile has no samples".to_string(),
        ));
    }
    Ok(stacks_to_pprof(name, "samples", sample_unit, stacks))
}
