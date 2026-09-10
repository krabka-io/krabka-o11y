use super::{
    BTreeMap, INDEX_SHARD_FORMAT_VERSION, INDEX_SHARD_MAGIC, IndexShardPayload, Labels,
    push_ivarint, push_uvarint,
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
        for (name, value) in labels.iter() {
            let next = dictionary.len();
            dictionary.entry(name.clone()).or_insert(next);
            let next = dictionary.len();
            dictionary.entry(value.clone()).or_insert(next);
        }
    }
    // Ids are assigned on the sorted order rather than on first sight, so the
    // same index encodes to the same bytes whichever order its series were
    // registered in.
    let mut strings = dictionary.keys().cloned().collect::<Vec<_>>();
    strings.sort();
    for (id, text) in strings.iter().enumerate() {
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
            for (name, value) in labels.iter() {
                push_id(&mut out, &dictionary, name);
                push_id(&mut out, &dictionary, value);
            }
        }
    }

    push_len(&mut out, payload.blocks.len());
    let mut previous_min = 0_i64;
    for block in &payload.blocks {
        push_string(&mut out, &block.object_key);
        push_ivarint(&mut out, block.min_ts.wrapping_sub(previous_min));
        push_ivarint(&mut out, block.max_ts.wrapping_sub(block.min_ts));
        previous_min = block.min_ts;
        push_len(&mut out, block.row_count);
        push_len(&mut out, block.fingerprint_count);
        out.extend_from_slice(&block.fingerprint_digest.to_le_bytes());
        push_uvarint(&mut out, u64::from(block.level.get()));
    }

    push_len(&mut out, payload.postings.len());
    for (fingerprint, ordinals) in &payload.postings {
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

fn push_len(out: &mut Vec<u8>, len: usize) {
    push_uvarint(
        out,
        u64::try_from(len).expect("a length in memory fits a u64"),
    );
}

fn push_string(out: &mut Vec<u8>, text: &str) {
    push_len(out, text.len());
    out.extend_from_slice(text.as_bytes());
}

fn push_id(out: &mut Vec<u8>, dictionary: &BTreeMap<String, usize>, text: &str) {
    let id = dictionary
        .get(text)
        .copied()
        .expect("every label name and value was put in the dictionary");
    push_len(out, id);
}
