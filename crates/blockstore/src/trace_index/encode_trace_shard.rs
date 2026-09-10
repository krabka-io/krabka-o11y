use super::{
    BTreeMap, TRACE_SHARD_FORMAT_VERSION, TRACE_SHARD_MAGIC, TraceBlockStats, push_ivarint,
    push_uvarint,
};

/// Encodes one tenant's trace-block records as a shard payload.
///
/// Tag names and tag values go into a per-shard dictionary and each block
/// references them by id. A shard holds a slice of one tenant's blocks, and
/// those blocks repeat the same handful of service names and operation names,
/// so the dictionary is where most of the saving is. Timestamps are deltas
/// along the shard's own order, and the bloom bit words go out raw, which is
/// what they cost: a bloom is incompressible by construction, and it is the
/// reason a trace index is large.
///
/// `blocks` must be sorted, so that the same set of records encodes to the same
/// bytes and therefore to the same content-addressed object key whichever order
/// a merge assembled them in.
pub(crate) fn encode_trace_shard(tenant: &str, blocks: &[TraceBlockStats]) -> Vec<u8> {
    let mut dictionary = BTreeMap::new();
    for block in blocks {
        for tag in &block.tag_names {
            dictionary.insert(tag.clone(), 0_usize);
        }
        for (tag, values) in &block.tag_values {
            dictionary.insert(tag.clone(), 0);
            for value in values {
                dictionary.insert(value.clone(), 0);
            }
        }
    }
    // Ids are assigned on the sorted order rather than on first sight, so the
    // same records encode to the same bytes whichever order they arrived in.
    let strings = dictionary.keys().cloned().collect::<Vec<_>>();
    for (id, text) in strings.iter().enumerate() {
        dictionary.insert(text.clone(), id);
    }

    let mut out = Vec::new();
    out.extend_from_slice(&TRACE_SHARD_MAGIC);
    out.push(TRACE_SHARD_FORMAT_VERSION);
    push_string(&mut out, tenant);

    push_len(&mut out, strings.len());
    for text in &strings {
        push_string(&mut out, text);
    }

    push_len(&mut out, blocks.len());
    let mut previous_min = 0_i64;
    for block in blocks {
        push_string(&mut out, &block.object_key);
        push_ivarint(&mut out, block.min_ts.wrapping_sub(previous_min));
        push_ivarint(&mut out, block.max_ts.wrapping_sub(block.min_ts));
        previous_min = block.min_ts;
        push_len(&mut out, block.row_count);
        push_uvarint(&mut out, u64::from(block.level.get()));

        push_len(&mut out, block.tag_names.len());
        for tag in &block.tag_names {
            push_id(&mut out, &dictionary, tag);
        }

        push_len(&mut out, block.tag_values.len());
        for (tag, values) in &block.tag_values {
            push_id(&mut out, &dictionary, tag);
            push_len(&mut out, values.len());
            for value in values {
                push_id(&mut out, &dictionary, value);
            }
        }

        push_len(&mut out, block.bloom.shards.len());
        for shard in &block.bloom.shards {
            push_uvarint(&mut out, shard.num_bits);
            push_uvarint(&mut out, u64::from(shard.k));
            push_len(&mut out, shard.bits.len());
            for word in &shard.bits {
                out.extend_from_slice(&word.to_le_bytes());
            }
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
        .expect("every tag name and value was put in the dictionary");
    push_len(out, id);
}
