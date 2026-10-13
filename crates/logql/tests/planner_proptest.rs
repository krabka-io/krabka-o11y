//! Property tests for the `LogQL` stream planner's index pruning.
//!
//! `plan_stream_query` answers a stream selector from two indexes rather than
//! from the blocks themselves. `LabelIndex::match_series` is the optimised
//! half: it intersects the posting lists of the equality matchers first, and
//! only then evaluates the rest of the matchers against the series that
//! survive. `BlockIndex::match_blocks` then drops every block that misses the
//! time range or holds none of those series.
//!
//! Both halves are prunings, so both have a naive form to check against.
//!
//! `pruning_series_agrees_with_a_full_scan` compares the posting-list path
//! against the full scan of every series in the tenant, which uses
//! `LabelMatcher::matches`, the same rule the querier applies to a row it has
//! already read. A divergence is a wrong answer, not a slow one: the plan
//! would drop a stream the query selects.
//!
//! `pruning_series_distributes_over_conjunction` states the same thing without
//! a second matcher implementation: planning all the matchers at once has to
//! give what planning each one alone and intersecting gives.
//!
//! `pruning_blocks_agrees_with_a_full_scan` compares the block pruning against
//! the full scan of every block in the index.

use std::collections::{BTreeMap, BTreeSet};

use krabka_blockstore::{
    BlockDescriptor, BlockKey, LabelIndex, LogBlockIndex, LogLabels, LogSeriesFingerprint,
    TimeRange,
};
use krabka_logql::{LabelMatcher, MatchOp, StreamQuery, plan_stream_query};
use proptest::prelude::*;

const TENANT: &str = "tenant-a";
/// A second tenant, so that a plan that ignores the tenant scope shows up as a
/// wrong answer rather than as an equal one.
const OTHER_TENANT: &str = "tenant-b";

/// A small label space. Reuse across series is the point: it is what gives the
/// posting lists more than one fingerprint to intersect.
const LABEL_NAMES: &[&str] = &["app", "env", "region"];
const LABEL_VALUES: &[&str] = &["api", "web", "worker", "prod", "dev", ""];

/// One series' labels. A label is present or absent independently, so the
/// generated set covers series that miss a label the query names.
fn arb_labels() -> impl Strategy<Value = LogLabels> {
    prop::collection::vec(
        prop::option::of(prop::sample::select(LABEL_VALUES)),
        LABEL_NAMES.len(),
    )
    .prop_map(|values| {
        let mut labels = BTreeMap::new();
        for index in 0..LABEL_NAMES.len() {
            if let Some(value) = values[index] {
                labels.insert(LABEL_NAMES[index].to_owned(), value.to_owned());
            }
        }
        labels
    })
}

/// One label matcher. The regex alphabet stays small and valid, since an
/// invalid regex is rejected at construction and is the parser's problem, not
/// the planner's.
fn arb_matcher() -> impl Strategy<Value = LabelMatcher> {
    let mut matchers = Vec::new();
    let operations = [
        MatchOp::Equal,
        MatchOp::NotEqual,
        MatchOp::RegexEqual,
        MatchOp::RegexNotEqual,
    ];
    let values = ["api", "web", "", "a.*", ".*", "(api|web)", "pro."];
    for name in LABEL_NAMES {
        for operation in operations {
            for value in values {
                matchers.push(
                    LabelMatcher::new(*name, operation, value)
                        .expect("the matcher alphabet is valid"),
                );
            }
        }
    }
    prop::sample::select(matchers)
}

fn arb_matchers() -> impl Strategy<Value = Vec<LabelMatcher>> {
    prop::collection::vec(arb_matcher(), 1..4)
}

/// A block: a time range, and the series it holds, given as indexes into the
/// series list so that a block always names series that exist.
#[derive(Debug)]
struct BlockSpec {
    start_ms: i64,
    end_ms: i64,
    series_indices: Vec<prop::sample::Index>,
    for_tenant: bool,
}

prop_compose! {
    fn arb_block_spec()
        (start_ms in 0_i64..1_000,
         end_ms in 0_i64..1_000,
         series_indices in prop::collection::vec(any::<prop::sample::Index>(), 0..4),
         for_tenant in any::<bool>()) -> BlockSpec {
        BlockSpec { start_ms, end_ms, series_indices, for_tenant }
    }
}

/// The whole world a plan runs against: the tenant's series, some series under
/// another tenant, and the blocks.
#[derive(Debug)]
struct QueryBounds {
    first_ms: i64,
    second_ms: i64,
}

#[derive(Debug)]
struct World {
    tenant_series: Vec<LogLabels>,
    other_series: Vec<LogLabels>,
    block_specs: Vec<BlockSpec>,
    query_bounds: QueryBounds,
}

#[derive(Debug)]
struct PlanCase {
    world: World,
    matchers: Vec<LabelMatcher>,
}

prop_compose! {
    fn arb_world()
        (tenant_series in prop::collection::vec(arb_labels(), 1..8),
         other_series in prop::collection::vec(arb_labels(), 0..3),
         block_specs in prop::collection::vec(arb_block_spec(), 0..6),
         first_ms in 0_i64..1_000,
         second_ms in 0_i64..1_000) -> World {
        World {
            tenant_series,
            other_series,
            block_specs,
            query_bounds: QueryBounds { first_ms, second_ms },
        }
    }
}

prop_compose! {
    fn arb_plan_case()
        (world in arb_world(), matchers in arb_matchers()) -> PlanCase {
        PlanCase { world, matchers }
    }
}

struct Fixture {
    label_index: LabelIndex,
    block_index: LogBlockIndex,
    time_range: TimeRange,
}

impl Fixture {
    fn build(world: &World) -> Self {
        let mut label_index = LabelIndex::default();
        let fingerprints: Vec<LogSeriesFingerprint> = world
            .tenant_series
            .iter()
            .map(|labels| label_index.insert_series(TENANT, labels.clone()))
            .collect();
        for labels in &world.other_series {
            label_index.insert_series(OTHER_TENANT, labels.clone());
        }

        let mut block_index = LogBlockIndex::default();
        for at in 0..world.block_specs.len() {
            let spec = &world.block_specs[at];
            let range = time_range(spec.start_ms, spec.end_ms);
            let tenant = if spec.for_tenant {
                TENANT
            } else {
                OTHER_TENANT
            };
            let key = BlockKey::new(
                tenant,
                0,
                i64::try_from(at).expect("a small block index fits in an i64"),
                i64::try_from(at).expect("a small block index fits in an i64"),
                range,
            );
            let members: BTreeSet<LogSeriesFingerprint> = spec
                .series_indices
                .iter()
                .map(|index| *index.get(&fingerprints))
                .collect();
            block_index.insert(BlockDescriptor::new(key, members));
        }

        Self {
            label_index,
            block_index,
            time_range: time_range(world.query_bounds.first_ms, world.query_bounds.second_ms),
        }
    }

    fn plan(&self, matchers: Vec<LabelMatcher>) -> krabka_logql::StreamPlan {
        plan_stream_query(
            TENANT,
            self.time_range,
            StreamQuery {
                matchers,
                pipeline: Vec::new(),
            },
            &self.label_index,
            &self.block_index,
        )
        .expect("a matcher built by the generator plans")
    }
}

/// A time range from two bounds in either order. `TimeRange::new` rejects an
/// end before its start.
fn time_range(a: i64, b: i64) -> TimeRange {
    TimeRange::new(a.min(b), a.max(b)).expect("the start is not after the end")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// The posting-list pruning returns what a full scan of the tenant's
    /// series returns.
    #[test]
    fn pruning_series_agrees_with_a_full_scan(case in arb_plan_case()) {
        let PlanCase { world, matchers } = case;
        let fixture = Fixture::build(&world);
        let plan = fixture.plan(matchers.clone());

        let want: BTreeSet<LogSeriesFingerprint> = fixture
            .label_index
            .tenant_series(TENANT)
            .into_iter()
            .filter(|series| {
                matchers
                    .iter()
                    .all(|matcher| matcher.matches(&series.labels))
            })
            .map(|series| series.fingerprint)
            .collect();

        prop_assert_eq!(plan.fingerprints, want);
    }

    /// Planning every matcher at once gives what planning each one alone and
    /// intersecting them gives.
    #[test]
    fn pruning_series_distributes_over_conjunction(
        case in arb_plan_case(),
    ) {
        let PlanCase { world, matchers } = case;
        let fixture = Fixture::build(&world);
        let together = fixture.plan(matchers.clone()).fingerprints;

        let mut apart: Option<BTreeSet<LogSeriesFingerprint>> = None;
        for matcher in matchers {
            let one = fixture.plan(vec![matcher]).fingerprints;
            apart = Some(match apart {
                Some(current) => current.intersection(&one).copied().collect(),
                None => one,
            });
        }

        prop_assert_eq!(together, apart.expect("the generator makes at least one matcher"));
    }

    /// The block pruning returns what a full scan of the block index returns:
    /// this tenant's blocks, overlapping the query range, holding at least one
    /// selected series.
    #[test]
    fn pruning_blocks_agrees_with_a_full_scan(case in arb_plan_case()) {
        let PlanCase { world, matchers } = case;
        let fixture = Fixture::build(&world);
        let plan = fixture.plan(matchers);

        let want: Vec<BlockDescriptor> = fixture
            .block_index
            .blocks()
            .iter()
            .filter(|block| {
                block.key.tenant == TENANT
                    && block.key.time_range.overlaps(fixture.time_range)
                    && !plan.fingerprints.is_empty()
                    && block
                        .fingerprints
                        .iter()
                        .any(|fingerprint| plan.fingerprints.contains(fingerprint))
            })
            .cloned()
            .collect();

        prop_assert!(plan.blocks == want, "the plan pruned a different block set");
    }
}
