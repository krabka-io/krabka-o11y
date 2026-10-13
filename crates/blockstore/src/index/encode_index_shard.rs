use super::{
    BTreeMap, INDEX_SHARD_FORMAT_VERSION, INDEX_SHARD_MAGIC, IndexShardPayload, Labels,
    push_dictionary_id, push_len, push_string, push_uvarint,
};

/// Encodes one shard.
///
/// Three things do the work that a `serde_json` document could not. Label
/// names and values go into a per-shard dictionary and each series references
/// them by id, which is what collapses the cost of a hundred thousand series
/// that share five label names and a handful of values. Block timestamps are
/// stored as deltas along the shard's own time order, and posting ordinals as
/// deltas along their ascending run, so both are a byte each in the ordinary
/// case. And the label postings and the label-value sets are not stored at
/// all: both are functions of the series, and replaying the series on load
/// rebuilds them exactly.
pub(crate) fn encode_index_shard(payload: &IndexShardPayload<'_>) -> Vec<u8> {
    let mut dictionary = BTreeMap::new();
    for fingerprint in &payload.selected {
        let Some(labels) = payload.series.get(fingerprint) else {
            continue;
        };
        for pair in labels.iter() {
            let next = dictionary.len();
            dictionary.entry(pair.name.to_string()).or_insert(next);
            let next = dictionary.len();
            dictionary.entry(pair.value.to_string()).or_insert(next);
        }
    }
    // Ids are assigned on the sorted order rather than on first sight, so the
    // same index encodes to the same bytes whichever order its series were
    // registered in.
    let mut strings = dictionary.keys().cloned().collect::<Vec<_>>();
    strings.sort();
    for id in 0..strings.len() {
        let text = &strings[id];
        dictionary.insert(text.clone(), id);
    }

    let mut out = Vec::new();
    out.extend_from_slice(&INDEX_SHARD_MAGIC);
    out.push(INDEX_SHARD_FORMAT_VERSION);
    push_string(&mut out, payload.tenant);

    push_len(&mut out, strings.len());
    for text in &strings {
        push_string(&mut out, text);
    }

    push_len(&mut out, payload.selected.len());
    for fingerprint in &payload.selected {
        out.extend_from_slice(&fingerprint.to_le_bytes());
        let labels = payload.series.get(fingerprint);
        let count = labels.map_or(0, Labels::len);
        push_len(&mut out, count);
        if let Some(labels) = labels {
            for pair in labels.iter() {
                push_dictionary_id(&mut out, &dictionary, pair.name);
                push_dictionary_id(&mut out, &dictionary, pair.value);
            }
        }
    }

    push_len(&mut out, payload.blocks.len());
    let mut previous_min = 0_i64;
    for block in &payload.blocks {
        super::push_shard_block_prefix(
            &mut out,
            &block.object_key,
            block.min_ts,
            block.max_ts,
            block.row_count,
            block.level,
            &mut previous_min,
        );
        push_len(&mut out, block.fingerprint_count);
        out.extend_from_slice(&block.fingerprint_digest.to_le_bytes());
    }

    push_len(&mut out, payload.postings.len());
    for fingerprint in payload.postings.keys() {
        let ordinals = &payload.postings[fingerprint];
        out.extend_from_slice(&fingerprint.to_le_bytes());
        push_len(&mut out, ordinals.len());
        let mut previous = 0_u32;
        for ordinal in ordinals {
            push_uvarint(&mut out, u64::from(ordinal.wrapping_sub(previous)));
            previous = *ordinal;
        }
    }

    out
}
