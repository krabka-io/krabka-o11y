//! Fixture builders shared by the benchmarks.
//!
//! A benchmark is a claim about a measurement, and a measurement over data the
//! run invented differently each time is not one. Everything here is generated
//! from a fixed seed with the small PRNG below, so two runs of the same
//! benchmark read the same bytes and a difference between them is a difference
//! in the code.
//!
//! The sizes are the point. The corpora this repository already has are the
//! right shape and the wrong order of magnitude: a differential suite seeds
//! hundreds of series to check an answer, which is what it is for. The design
//! this stack rests on -- a columnar block store over object storage -- pays
//! off at cardinality and volume, and its failure modes are invisible below
//! that. So the generators here are parameterised by the number that matters
//! for each hot path, and the benchmarks sweep it: series count for the index,
//! row count for a block, sample count for a range query, span count for a
//! filter. A curve across those sizes says whether an operation is linear in
//! the thing it is supposed to be linear in, which is a stronger statement
//! than any single wall-clock number, and it is the statement that survives
//! being measured on a machine other people are also using.

pub mod blocks;
pub mod index;
pub mod metrics;
pub mod profiles;
pub mod spans;

/// A seeded `SplitMix64`, so a fixture is the same on every run and machine.
///
/// Not a general-purpose generator: it exists to make label values, trace ids
/// and sample noise reproducible without adding a dependency on `rand`, whose
/// output is not guaranteed stable across releases and would therefore change
/// a fixture underneath a checked-in baseline.
pub struct Seeded(u64);

impl Seeded {
    /// A generator started from `seed`.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// The next value in the sequence.
    pub const fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// The next value, reduced to `0..bound`.
    ///
    /// The modulo bias is immaterial: these values pick a label out of a list,
    /// and a fixture does not need a uniform distribution, only a fixed one.
    ///
    /// # Panics
    /// Panics when `bound` is zero, which is a fixture asking for a choice
    /// from an empty set, and on a platform where a `usize` does not fit a
    /// `u64`, which is not one this workspace builds for.
    pub fn next_below(&mut self, bound: usize) -> usize {
        let bound = u64::try_from(bound).expect("a usize fits a u64");
        assert!(bound > 0, "a bound of zero has nothing to choose from");
        usize::try_from(self.next_u64() % bound).expect("a value below a usize fits a usize")
    }

    /// The next value as a sample-shaped float, in `0.0..1024.0`.
    ///
    /// Built from a `u32` rather than by casting the whole `u64`, so every
    /// value it produces is exactly representable and the conversion loses
    /// nothing.
    ///
    /// # Panics
    /// Panics if the high half of a `u64` does not fit a `u32`, which it does.
    pub fn next_sample(&mut self) -> f64 {
        let bits = u32::try_from(self.next_u64() >> 32).expect("the high half of a u64 is a u32");
        f64::from(bits) / f64::from(u32::MAX) * 1024.0
    }
}
