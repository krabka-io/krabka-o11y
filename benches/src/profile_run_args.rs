//! Command-line arguments shared by the profiling examples.

/// The leading arguments every profiling example takes: how many streams the
/// fixture holds, how many timed iterations to run, and which case to run.
pub struct ProfileRunArgs {
    /// Number of streams the fixture is generated with.
    pub streams: usize,
    /// Number of timed iterations after the verification run.
    pub iterations: usize,
    /// Name of the case the fixture runs.
    pub case: String,
}

impl ProfileRunArgs {
    /// Reads the stream count, the iteration count and the case name from the
    /// front of `args`, leaving any further arguments for the caller.
    ///
    /// # Errors
    /// Returns an error when a count is not an unsigned integer.
    ///
    /// # Panics
    /// Panics when one of the three arguments is missing.
    pub fn take_from(
        args: &mut impl Iterator<Item = String>,
    ) -> Result<Self, std::num::ParseIntError> {
        let streams = args.next().expect("stream count").parse::<usize>()?;
        let iterations = args.next().expect("iteration count").parse::<usize>()?;
        let case = args.next().expect("case name");
        Ok(Self {
            streams,
            iterations,
            case,
        })
    }
}
