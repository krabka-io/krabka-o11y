# Sorted compaction merge selection — issue 386

The merge uses a tournament tree of run indices. Initialization takes linear work in the run count. Each subsequent slice refreshes only its consumed run and takes logarithmic work to update the tree and find the next key. The existing binary search still emits a bulk slice when possible. Equal keys retain input-run order.

## Method

- Date: 2026-10-19, America/Los_Angeles.
- Host: x86_64 Intel Xeon Platinum 8573C, Linux 6.18.55, four CPU quota, 16 GiB memory.
- Toolchain: rustc 1.99.1 (58cf8f9c3 2026-10-13).
- Before: `sorted_merge.rs` from `efb9708daef226f52d86f0b74402f6806389963b`.
- After: this change. The driver, dependencies, lockfile and build options are identical.
- Inputs: interleaved and disjoint runs at 2, 8, 24 and 64 runs. Each run yields batches of at most 1,024 rows.
- Total rows: 65,536, except the 24-run fixture has 65,544 rows. Each row has three unsigned integer columns and one declared sort-key column.
- Each process verifies every output row against a stable sort of its input ledger, including equal-key order.
- Each process runs timed merges, then one separate merge with allocation accounting. A current-thread Tokio runtime keeps all work on the measured thread.
- The initial matrix has three process repetitions per case, each with 20 timed merges. Before/after execution order alternates.
- Disjoint cases have three additional repetitions with 200 timed merges to extend the shorter measurements. The table uses these longer captures.
- The 64-run interleaved case has two additional repetitions with 20 timed merges after a large outlier. Its median includes all five repetitions.
- All 76 process measurements remain in the CSV. No compiler runs during measurement.

The timer includes merge construction, key encoding, selection, output concatenation and output consumption. It excludes fixture construction and verification. Allocation accounting stays disabled during timing. The instrumented system allocator remains linked in both executables.

The [raw CSV](sorted-merge-386.csv) records elapsed time, allocation count, total allocated bytes, peak additional live bytes, process CPU time and peak RSS. Allocation measurements cover one merge and exclude retained input buffers and the expected-output ledger. CPU and RSS come from Linux `wait4` resource usage for each process. They include fixture construction, verification, timed iterations and the allocation pass. These measurements cover memory-backed merge inputs. They do not measure object storage or complete signal compaction.

## Build and run

The comparison uses the dev profile with the blockstore crate, driver and allocation counter at optimization level 3. Other dependencies remain unoptimized. These controlled measurements are not production latency estimates or CI performance budgets.

```bash
CARGO_TARGET_DIR=target \
CARGO_PROFILE_DEV_DEBUG=line-tables-only \
CARGO_PROFILE_DEV_SPLIT_DEBUGINFO=packed \
cargo build --manifest-path benches/Cargo.toml --example sorted_merge_profile --locked \
  --config 'profile.dev.package.krabka-blockstore.opt-level=3' \
  --config 'profile.dev.package.krabka-o11y-benches.opt-level=3' \
  --config 'profile.dev.package.allocation-counter.opt-level=3'
target/debug/examples/sorted_merge_profile 20
```

To reproduce the before build, use the same driver and lockfile but restore `crates/blockstore/src/merge/sorted_merge.rs` from the base commit. Save the before executable, restore the fixed source, and build the after executable. Build both before measurement. Run each executable with `20 interleaved 64`, for example, to select one case for a CPU and RSS capture. Repeat for each pattern and run count. Use `200 disjoint 64`, for example, for the longer disjoint capture.

## Results

The table reports process medians. Elapsed time is per merge. Process CPU is user plus system time for the full process scope above.

| Pattern | Runs | Timed merges | Before ms | After ms | Before CPU s | After CPU s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| interleaved | 2 | 20 | 77.05 | 75.74 | 1.696 | 1.699 |
| interleaved | 8 | 20 | 161.73 | 150.58 | 3.570 | 3.315 |
| interleaved | 24 | 20 | 260.40 | 197.53 | 5.702 | 4.363 |
| interleaved | 64 | 20 | 540.28 | 215.60 | 11.694 | 4.800 |
| disjoint | 2 | 200 | 7.93 | 6.95 | 1.605 | 1.426 |
| disjoint | 8 | 200 | 8.94 | 6.31 | 1.813 | 1.290 |
| disjoint | 24 | 200 | 9.73 | 6.74 | 1.993 | 1.373 |
| disjoint | 64 | 200 | 8.35 | 6.12 | 1.688 | 1.229 |

For 64 interleaved runs, median elapsed time fell from 540.28 to 215.60 ms, and median process CPU fell 59%. Four of its five after samples fall between 172.54 and 217.46 ms. One after sample took 1,259.52 ms. Before samples range from 492.62 to 683.01 ms. The shared host also produced wide timing ranges in smaller cases, so their speedup estimates remain inconclusive.

Both patterns add exactly one allocation per merge. Total allocated bytes and peak additional live bytes increase by the tree buffer size:

| Runs | Added allocations | Added allocated bytes | Added peak live bytes |
| ---: | ---: | ---: | ---: |
| 2 | 1 | 32 | 32 |
| 8 | 1 | 128 | 128 |
| 24 | 1 | 512 | 512 |
| 64 | 1 | 1024 | 1024 |

At 64 interleaved runs, allocations change from 262,831 to 262,832. Total allocated bytes change from 36,817,171 to 36,818,195. Peak additional live bytes change from 4,998,616 to 4,999,640.

Median process RSS is 17,180 KiB before and 17,164 KiB after. RSS varies between processes and cannot resolve the 1 KiB tree buffer reliably. The allocation counts also confirm that disjoint inputs retain bulk slices.

## Validation

- `cargo test --locked -p krabka-blockstore --lib` passes all 338 tests.
- The four regression tests pass. Their 84-case output matrix covers zero, one and many runs, equal keys, empty batches, batch boundaries and row limits of zero, one, seven and 128. Separate tests cover exhausted streams, initial and refill errors, retries and a cancelled refill.
- `cargo clippy --locked -p krabka-blockstore --test sorted_merge -- -D warnings` passes.
- `CARGO_TARGET_DIR=target cargo clippy --locked --manifest-path benches/Cargo.toml --example sorted_merge_profile -- -D warnings` passes.
- `bazel run //tools/format` and `bazel run //tools/format:format.check` pass.
