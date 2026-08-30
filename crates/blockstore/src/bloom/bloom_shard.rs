use super::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct BloomShard {
    pub(crate) bits: Vec<u64>,
    pub(crate) num_bits: u64,
    pub(crate) k: u32,
}

impl BloomShard {
    pub(crate) fn new(expected_items: usize, fp_rate: f64) -> Self {
        let n = expected_items
            .max(1)
            .to_f64()
            .expect("usize is representable as a finite f64");
        let fp_rate = fp_rate.clamp(0.000_001, 0.5);
        let m = (-(n * fp_rate.ln()) / (std::f64::consts::LN_2 * std::f64::consts::LN_2))
            .ceil()
            .max(64.0);
        let k = ((m / n) * std::f64::consts::LN_2)
            .round()
            .max(1.0)
            .to_u32()
            .expect("bounded bloom probe count fits u32");
        let num_bits = m.to_u64().expect("bounded bloom size fits u64");
        let words = usize::try_from(num_bits.div_ceil(64)).unwrap_or(usize::MAX);
        Self {
            bits: vec![0_u64; words],
            num_bits,
            k,
        }
    }

    /// Rejects a deserialized shard that would panic on lookup. Such a shard
    /// has `num_bits == 0`, which is a divide-by-zero in `probes`, or a `bits`
    /// vector too short to hold every bit position, which is an out-of-bounds
    /// index in `maybe_contains` or `insert`.
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.num_bits == 0 {
            return Err("bloom shard num_bits must be non-zero".into());
        }
        let required_words = usize::try_from(self.num_bits.div_ceil(64)).unwrap_or(usize::MAX);
        if self.bits.len() < required_words {
            return Err(format!(
                "bloom shard bits too short: have {}, need {required_words}",
                self.bits.len()
            ));
        }
        Ok(())
    }

    pub(crate) fn probes(&self, trace_id: &[u8; 16]) -> impl Iterator<Item = u64> + '_ {
        let h1 = u64::from(fnv1_32(trace_id));
        let h2 = u64::from(fnv1a_32(trace_id)) | 1;
        let num_bits = self.num_bits;
        (0..u64::from(self.k)).map(move |i| h1.wrapping_add(i.wrapping_mul(h2)) % num_bits)
    }

    pub(crate) fn insert(&mut self, trace_id: &[u8; 16]) {
        let probes: Vec<u64> = self.probes(trace_id).collect();
        for bit in probes {
            self.bits[(bit / 64) as usize] |= 1_u64 << (bit % 64);
        }
    }

    pub(crate) fn maybe_contains(&self, trace_id: &[u8; 16]) -> bool {
        self.probes(trace_id)
            .all(|bit| self.bits[(bit / 64) as usize] & (1_u64 << (bit % 64)) != 0)
    }
}
