use super::*;

pub(crate) fn trie_to_pprof(
    name: &str,
    sample_unit: &str,
    body: &[u8],
    limits: LegacyDecodeLimits,
) -> Result<PprofProfile, ProfilesError> {
    let mut pos = 0;
    let mut stacks = BTreeMap::<Vec<(String, i32)>, i64>::new();
    let mut node_count = 0_usize;
    let mut path_bytes = 0_usize;

    // Explicit work-stack: each frame is a node prefix paired with the number
    // of sibling children that still need to be visited at this depth. The
    // stack length is the live recursion depth, hard-capped below so a deeply
    // nested payload cannot overflow the native stack.
    //
    // The top-level forest is modeled as a synthetic root whose "remaining"
    // count is consumed one node per outer step until the payload is exhausted.
    let mut work: Vec<TrieFrame> = Vec::new();

    loop {
        // Unwind any completed parents first so `work.len()` reflects true live
        // depth and the exhaustion check below isn't tripped by spent frames.
        while let Some(frame) = work.last() {
            if frame.remaining == 0 {
                work.pop();
            } else {
                break;
            }
        }

        if pos >= body.len() {
            if work.is_empty() {
                break;
            }
            // Out of bytes but the work-stack still expects children: malformed.
            return Err(ProfilesError::Decode(
                "trie payload ended before all declared children".to_string(),
            ));
        }

        if work.len() >= limits.max_trie_depth {
            return Err(ProfilesError::Decode(
                "trie profile exceeds maximum depth".to_string(),
            ));
        }

        node_count += 1;
        if node_count > limits.max_nodes {
            return Err(ProfilesError::Decode(
                "trie profile exceeds node budget".to_string(),
            ));
        }

        let prefix: &[u8] = work.last().map_or(&[][..], |frame| frame.key.as_slice());

        let suffix_len = read_tree_varint(body, &mut pos, "trie node suffix length")?;
        let suffix_len = usize::try_from(suffix_len).map_err(|err| {
            ProfilesError::Decode(format!("trie node suffix length does not fit usize: {err}"))
        })?;
        let suffix_end = pos.checked_add(suffix_len).ok_or_else(|| {
            ProfilesError::Decode("trie node suffix length overflows payload offset".to_string())
        })?;
        if suffix_end > body.len() {
            return Err(ProfilesError::Decode(
                "trie node suffix length exceeds payload".to_string(),
            ));
        }

        let mut key = Vec::with_capacity(prefix.len().saturating_add(suffix_len));
        key.extend_from_slice(prefix);
        key.extend_from_slice(&body[pos..suffix_end]);
        pos = suffix_end;

        // Charge the materialized key length against the cumulative budget.
        // Long shared prefixes are copied into every descendant, so a hostile
        // payload can amplify key storage well beyond the input size.
        path_bytes = path_bytes.saturating_add(key.len());
        if path_bytes > limits.max_path_bytes.bytes_usize() {
            return Err(ProfilesError::Decode(
                "trie profile exceeds path-bytes budget".to_string(),
            ));
        }

        let value = read_tree_varint(body, &mut pos, "trie node value")?;
        let children_len = read_tree_varint(body, &mut pos, "trie node children length")?;
        let children_len = usize::try_from(children_len).map_err(|err| {
            ProfilesError::Decode(format!(
                "trie node children length does not fit usize: {err}"
            ))
        })?;
        if children_len > body.len().saturating_sub(pos) + 1 {
            return Err(ProfilesError::Decode(
                "trie node children length exceeds remaining payload".to_string(),
            ));
        }

        if value > 0 {
            let value = i64::try_from(value).map_err(|err| {
                ProfilesError::Decode(format!("trie node value does not fit i64: {err}"))
            })?;
            let key_str = std::str::from_utf8(&key)
                .map_err(|err| ProfilesError::Decode(format!("trie key is not UTF-8: {err}")))?;
            let frames = key_str
                .split(';')
                .filter(|frame| !frame.is_empty())
                .map(|frame| (frame.to_string(), 0))
                .collect::<Vec<_>>();
            if frames.is_empty() {
                return Err(ProfilesError::Decode(
                    "trie profile has an empty stack".to_string(),
                ));
            }
            *stacks.entry(frames).or_default() += value;
        }

        // This node consumed one of its parent's remaining child slots.
        if let Some(frame) = work.last_mut() {
            frame.remaining -= 1;
        }
        // Descend if this node declares children.
        if children_len > 0 {
            work.push(TrieFrame {
                key,
                remaining: children_len,
            });
        }
    }

    if stacks.is_empty() {
        return Err(ProfilesError::Decode(
            "trie profile has no samples".to_string(),
        ));
    }
    Ok(stacks_to_pprof(name, "samples", sample_unit, stacks))
}
