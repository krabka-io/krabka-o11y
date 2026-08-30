//! FNV-sharded trace-id bloom filter for index-less trace lookup.

use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    fn tid(n: u8) -> [u8; 16] {
        let mut t = [0u8; 16];
        t[0] = n;
        t[15] = n.wrapping_mul(7);
        t
    }

    #[test]
    fn no_false_negatives() {
        let mut b = ShardedTraceBloom::new(8, 64, 0.01);
        for n in 0..64_u8 {
            b.insert(&tid(n));
        }
        for n in 0..64_u8 {
            assert2::assert!(b.maybe_contains(&tid(n)));
        }
    }

    #[test]
    fn false_positive_rate_is_bounded() {
        let mut b = ShardedTraceBloom::new(16, 256, 0.01);
        for n in 0..=255_u8 {
            b.insert(&tid(n));
        }
        let mut fp = 0_u32;
        let mut probes = 0_u32;
        for n in 256_u32..4352 {
            let mut t = [0_u8; 16];
            t[0..4].copy_from_slice(&n.to_le_bytes());
            t[15] = 0xAB;
            probes += 1;
            if b.maybe_contains(&t) {
                fp += 1;
            }
        }
        let rate = f64::from(fp) / f64::from(probes);
        assert2::assert!(rate < 0.05);
    }

    #[test]
    fn shard_is_fnv_mod_count() {
        let b = ShardedTraceBloom::new(16, 64, 0.01);
        let t = tid(42);
        assert2::assert!(b.shard_of(&t) == (fnv1_32(&t) as usize) % 16);
    }

    #[test]
    fn match_all_bloom_has_no_false_negatives() {
        let b = ShardedTraceBloom::match_all_with_tempo_defaults();
        for n in 0..=255_u8 {
            assert2::assert!(b.maybe_contains(&tid(n)));
        }
    }

    #[test]
    fn fnv1_32_is_stable() {
        let h = fnv1_32(&[0_u8]);
        let expected = 2_166_136_261_u32.wrapping_mul(16_777_619);
        assert2::assert!(h == expected);
    }

    #[test]
    fn fnv1_32_xors_each_byte() {
        // FNV-1: hash = (hash * PRIME) ^ byte, applied per byte. A non-zero
        // second byte makes `^=` differ from `|=`: pin the exact two-byte hash
        // so swapping the xor for an or (or and) fails.
        const PRIME: u32 = 16_777_619;
        let h0 = 2_166_136_261_u32;
        let h1 = h0.wrapping_mul(PRIME) ^ 0x01;
        let expected = h1.wrapping_mul(PRIME) ^ 0xFF;
        assert2::assert!(fnv1_32(&[0x01, 0xFF]) == expected);
        // Guard: with `|=` the result would be h1.mul(PRIME) | 0xFF, which differs.
        let or_variant = h1.wrapping_mul(PRIME) | 0xFF;
        assert2::assert!(expected != or_variant);
    }

    #[test]
    fn fnv1a_32_xors_then_multiplies_each_byte() {
        // FNV-1a: hash = (hash ^ byte) * PRIME, per byte. Pin the exact value so
        // a `0`/`1` stub return or swapping the `^=` for `&=`/`|=` fails.
        const PRIME: u32 = 16_777_619;
        let h0 = 2_166_136_261_u32;
        let h1 = (h0 ^ 0x01).wrapping_mul(PRIME);
        let expected = (h1 ^ 0xFF).wrapping_mul(PRIME);
        check!(fnv1a_32(&[0x01, 0xFF]) == expected);
        check!(fnv1a_32(&[0x01, 0xFF]) != 0);
        check!(fnv1a_32(&[0x01, 0xFF]) != 1);
        // `&=` and `|=` produce different hashes for the same input.
        let and_variant = ((h0 & 0x01).wrapping_mul(PRIME) & 0xFF).wrapping_mul(PRIME);
        let or_variant = ((h0 | 0x01).wrapping_mul(PRIME) | 0xFF).wrapping_mul(PRIME);
        assert2::assert!(expected != and_variant);
        assert2::assert!(expected != or_variant);
    }

    #[test]
    fn probes_force_odd_step_with_or_one() {
        // `probes` uses h2 = fnv1a_32(id) | 1 as the step. Replacing the `| 1`
        // with `& 1` (step in {0,1}) or `^ 1` (flips the low bit) changes the
        // probe positions. Choose a trace id whose fnv1a hash has its low bit
        // SET so `| 1` and `^ 1` actually diverge, then pin the exact probes.
        let trace_id = tid(3);
        assert2::assert!(fnv1a_32(&trace_id) & 1 == 1);

        let shard = BloomShard::new(64, 0.01);
        let h1 = u64::from(fnv1_32(&trace_id));
        let h2 = u64::from(fnv1a_32(&trace_id)) | 1;
        let expected: Vec<u64> = (0..u64::from(shard.k))
            .map(|i| h1.wrapping_add(i.wrapping_mul(h2)) % shard.num_bits)
            .collect();
        let got: Vec<u64> = shard.probes(&trace_id).collect();
        assert2::assert!(got == expected);

        // The `& 1` and `^ 1` variants give a different probe sequence.
        let h2_and = u64::from(fnv1a_32(&trace_id)) & 1;
        let and_variant: Vec<u64> = (0..u64::from(shard.k))
            .map(|i| h1.wrapping_add(i.wrapping_mul(h2_and)) % shard.num_bits)
            .collect();
        let h2_xor = u64::from(fnv1a_32(&trace_id)) ^ 1;
        let xor_variant: Vec<u64> = (0..u64::from(shard.k))
            .map(|i| h1.wrapping_add(i.wrapping_mul(h2_xor)) % shard.num_bits)
            .collect();
        assert2::assert!(expected != and_variant);
        assert2::assert!(expected != xor_variant);
    }

    #[test]
    fn snapshot_round_trips() {
        let mut b = ShardedTraceBloom::new(4, 32, 0.01);
        b.insert(&tid(1));
        let json = serde_json::to_vec(&b).unwrap();
        let back: ShardedTraceBloom = serde_json::from_slice(&json).unwrap();
        assert2::assert!(back.maybe_contains(&tid(1)));
    }

    #[test]
    fn validate_accepts_constructed_bloom() {
        let b = ShardedTraceBloom::new(4, 32, 0.01);
        assert2::assert!(b.validate().is_ok());
    }

    #[test]
    fn validate_rejects_corrupt_deserialized_blooms() {
        for (_name, json) in [
            (
                "zero bit count",
                r#"{"shards":[{"bits":[],"num_bits":0,"k":1}]}"#,
            ),
            ("no shards", r#"{"shards":[]}"#),
            (
                "bit vector shorter than declared count",
                r#"{"shards":[{"bits":[0],"num_bits":128,"k":1}]}"#,
            ),
        ] {
            let bloom: ShardedTraceBloom = serde_json::from_str(json).unwrap();
            assert2::assert!(bloom.validate().is_err());
        }
    }
}

mod bloom_shard;
mod fnv1_32;
mod fnv1a_32;
mod sharded_trace_bloom;

use bloom_shard::BloomShard;
pub use fnv1_32::fnv1_32;
use fnv1a_32::fnv1a_32;
pub use sharded_trace_bloom::ShardedTraceBloom;
