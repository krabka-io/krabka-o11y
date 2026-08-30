//! Deduplicated on-block symbol DB artifact.

use std::{borrow::Cow, collections::HashMap};

use serde::{Deserialize, Serialize};
use serde_wincode::SerdeCompat;
use wincode::{Deserialize as WincodeDeserialize, Serialize as WincodeSerialize};

use crate::{
    error::ProfileError,
    frame::{Frame, SymbolSource},
};

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    /// `MappingSymbolization` packs four independent booleans into one byte and
    /// nothing round-tripped them. Every survivor here was a bit-level slip
    /// that an all-false or all-true case cannot see: `|=` written as `&=`
    /// clears the bit it meant to set, `& FLAG != 0` written as `| FLAG != 0`
    /// answers true for every mapping, `^` misreads only the mapping where that
    /// one flag is the only one set, and `1 << 2` shifted the other way makes a
    /// flag that can never be stored.
    ///
    /// Exhausting all sixteen combinations is what separates them: each bit is
    /// asserted set and clear, alone and alongside every other.
    #[test]
    fn mapping_symbolization_round_trips_every_flag_combination() {
        for bits in 0_u8..16 {
            let parts = (bits & 1 != 0, bits & 2 != 0, bits & 4 != 0, bits & 8 != 0);
            let flags = MappingSymbolization::from_parts(parts);
            check!(
                (
                    flags.has_functions(),
                    flags.has_filenames(),
                    flags.has_line_numbers(),
                    flags.has_inline_frames(),
                ) == parts,
                "combination {bits:#06b}"
            );
        }

        // Distinct combinations must not collapse onto the same byte, which is
        // what a flag stored at the wrong bit would do.
        let mut seen = std::collections::HashSet::new();
        for bits in 0_u8..16 {
            let parts = (bits & 1 != 0, bits & 2 != 0, bits & 4 != 0, bits & 8 != 0);
            check!(seen.insert(MappingSymbolization::from_parts(parts)));
        }
    }

    fn db_with_abc() -> (SymbolDb, [u32; 3]) {
        let mut db = SymbolDb::new();
        let mk = |db: &mut SymbolDb, name: &str| {
            let name_ref = db.intern_string(name);
            let filename_ref = db.intern_string(&format!("{name}.go"));
            let function = db.intern_function(FunctionRec {
                name: name_ref,
                system_name: name_ref,
                filename: filename_ref,
                start_line: 1,
            });
            db.intern_location(LocationRec {
                address: 0,
                mapping_id: 0,
                lines: vec![LineRec {
                    function_id: function,
                    line: 10,
                }],
            })
        };
        let a = mk(&mut db, "a");
        let b = mk(&mut db, "b");
        let c = mk(&mut db, "c");
        (db, [a, b, c])
    }

    #[test]
    fn string_zero_is_empty() {
        let db = SymbolDb::default();
        assert!(db.string(0) == "");
    }

    #[test]
    fn new_reserves_empty_string_slot() {
        let mut db = SymbolDb::new();
        let name = db.intern_string("name");

        check!(db.string(0) == "");
        check!(name == 1);
        check!(db.string(name) == "name");
    }

    #[test]
    fn identical_stacks_dedup_to_same_leaf() {
        let (mut db, [a, b, c]) = db_with_abc();
        let id1 = db.intern_stacktrace(0, &[a, b, c]);
        let id2 = db.intern_stacktrace(0, &[a, b, c]);
        assert!(id1 == id2);
    }

    #[test]
    fn intern_stacktrace_roots_first_node_at_sentinel_parent() {
        let (mut db, [a, b, _c]) = db_with_abc();
        let id = db.intern_stacktrace(0, &[a, b]);
        let part = db.partitions.get(&0).unwrap();

        check!(id == 1);
        check!(part.nodes[0].parent == -1);
        check!(part.nodes[1].parent == 0);
    }

    #[test]
    fn resolve_stops_at_corrupt_parent_cycle() {
        let (mut db, [a, b, _c]) = db_with_abc();
        let id = db.intern_stacktrace(0, &[a, b]);
        db.partitions.get_mut(&0).unwrap().nodes[0].parent = 1;

        let frames = db.resolve(0, id);
        let names: Vec<&str> = frames.iter().map(|frame| frame.function.as_str()).collect();
        assert!(names == vec!["a", "b"]);
    }

    #[test]
    fn divergent_stacks_get_distinct_leaves_but_share_prefix() {
        let (mut db, [a, b, c]) = db_with_abc();
        let abc = db.intern_stacktrace(0, &[a, b, c]);
        let ab = db.intern_stacktrace(0, &[a, b]);
        assert!(abc != ab);
        let other = db.intern_stacktrace(1, &[a, b, c]);
        assert!(db.resolve(1, other).len() == 3);
    }

    #[test]
    fn empty_stack_interns_to_sentinel_and_resolves_to_no_frames() {
        let (mut db, [a, _b, _c]) = db_with_abc();
        // The first real stacktrace owns node 0; an empty stack must not collide
        // with it (which would borrow node 0's root frame) — it gets the sentinel.
        let first = db.intern_stacktrace(0, &[a]);
        let empty = db.intern_stacktrace(0, &[]);
        check!(first == 0);
        check!(empty == EMPTY_STACKTRACE_ID);
        check!(empty != first);
        check!(db.resolve(0, empty).is_empty());
        check!(db.raw_locations(0, empty).is_empty());
        // The real stack still resolves to its single frame.
        check!(db.resolve(0, first).len() == 1);
    }

    #[test]
    fn resolve_climbs_leaf_to_root() {
        let (mut db, [a, b, c]) = db_with_abc();
        let id = db.intern_stacktrace(0, &[a, b, c]);
        let frames = db.resolve(0, id);
        let names: Vec<&str> = frames.iter().map(|frame| frame.function.as_str()).collect();
        assert!(names == vec!["a", "b", "c"]);
    }

    #[test]
    fn invalid_large_stacktrace_ids_resolve_to_empty() {
        let (mut db, [a, b, c]) = db_with_abc();
        let _ = db.intern_stacktrace(0, &[a, b, c]);
        let invalid = u32::try_from(i64::from(i32::MAX) + 1).unwrap();

        assert!(db.resolve(0, invalid).is_empty());

        let mut raw_db = SymbolDb::new();
        let filename = raw_db.intern_string("/bin/app");
        let build_id = raw_db.intern_string("build");
        let mapping = raw_db.intern_mapping(MappingRec {
            memory_start: 0,
            memory_limit: 0x1000,
            file_offset: 0,
            filename,
            build_id,
            symbolization: MappingSymbolization::default(),
        });
        let loc_a = raw_db.intern_location(LocationRec {
            address: 0x10,
            mapping_id: mapping,
            lines: Vec::new(),
        });
        let loc_b = raw_db.intern_location(LocationRec {
            address: 0x20,
            mapping_id: mapping,
            lines: Vec::new(),
        });
        let _ = raw_db.intern_stacktrace(0, &[loc_a, loc_b]);

        assert!(raw_db.raw_locations(0, invalid).is_empty());
    }

    #[test]
    fn resolve_expands_inlined_frames_innermost_first() {
        let mut db = SymbolDb::new();
        let outer = db.intern_string("outer");
        let inner = db.intern_string("inner");
        let outer_fn = db.intern_function(FunctionRec {
            name: outer,
            system_name: outer,
            filename: 0,
            start_line: 1,
        });
        let inner_fn = db.intern_function(FunctionRec {
            name: inner,
            system_name: inner,
            filename: 0,
            start_line: 1,
        });
        let loc = db.intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![
                LineRec {
                    function_id: inner_fn,
                    line: 5,
                },
                LineRec {
                    function_id: outer_fn,
                    line: 9,
                },
            ],
        });
        let id = db.intern_stacktrace(0, &[loc]);
        let frames = db.resolve(0, id);
        let names: Vec<&str> = frames.iter().map(|frame| frame.function.as_str()).collect();
        assert!(names == vec!["inner", "outer"]);
    }

    #[test]
    fn resolve_drops_go_shape_type_parameters_like_pyroscope() {
        let mut db = SymbolDb::new();
        let name = db.intern_string(
            "github.com/dgraph-io/ristretto/v2.(*Cache[go.shape.string,go.shape.bool]).processItems",
        );
        let file = db.intern_string("cache.go");
        let function = db.intern_function(FunctionRec {
            name,
            system_name: name,
            filename: file,
            start_line: 1,
        });
        let location = db.intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id: function,
                line: 42,
            }],
        });
        let id = db.intern_stacktrace(0, &[location]);

        let frames = db.resolve(0, id);

        assert!(frames[0].function == "github.com/dgraph-io/ristretto/v2.(*Cache).processItems");
    }

    #[test]
    fn drop_go_type_parameters_handles_multiple_nested_and_unclosed_shapes() {
        assert!(
            drop_go_type_parameters("pkg.(*Cache[go.shape.string]).Get[go.shape.int]").as_ref()
                == "pkg.(*Cache).Get"
        );
        assert!(
            drop_go_type_parameters("pkg.F[go.shape.struct{Field [go.shape.int]}].G").as_ref()
                == "pkg.F.G"
        );
        let unclosed = "pkg.F[go.shape.string";
        assert!(drop_go_type_parameters(unclosed).as_ref() == unclosed);
        let ordinary_generic = "pkg.F[int]";
        assert!(drop_go_type_parameters(ordinary_generic).as_ref() == ordinary_generic);
    }

    #[test]
    fn encode_decode_round_trips() {
        let (mut db, [a, b, c]) = db_with_abc();
        let id = db.intern_stacktrace(0, &[a, b, c]);
        let bytes = db.encode();
        let mut back = SymbolDb::decode(&bytes).unwrap();
        check!(back.resolve(0, id) == db.resolve(0, id));
        check!(back.intern_string("a") == db.intern_string("a"));
        check!(back.intern_stacktrace(0, &[a, b, c]) == id);
    }

    #[test]
    fn symbol_source_impl_delegates_to_resolve() {
        let (mut db, [a, b, c]) = db_with_abc();
        let id = db.intern_stacktrace(0, &[a, b, c]);
        let source: &dyn SymbolSource = &db;
        assert!(source.resolve(0, id) == db.resolve(0, id));
    }

    #[test]
    fn copy_partition_preserves_stacktrace_ids_with_remapped_symbols() {
        let (mut source, [a, b, _]) = db_with_abc();
        let id = source.intern_stacktrace(0, &[a, b]);
        let mut dest = SymbolDb::new();
        let pre_name = dest.intern_string("preexisting");
        let pre_fn = dest.intern_function(FunctionRec {
            name: pre_name,
            system_name: pre_name,
            filename: 0,
            start_line: 1,
        });
        let _ = dest.intern_location(LocationRec {
            address: 0xff,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id: pre_fn,
                line: 1,
            }],
        });

        dest.copy_partition_from(&source, 0, 17).unwrap();

        assert!(dest.resolve(17, id) == source.resolve(0, id));
    }

    #[test]
    fn copy_partition_rebuilds_children_and_rejects_nonempty_destination() {
        let (mut source, [a, b, _]) = db_with_abc();
        let id = source.intern_stacktrace(0, &[a, b]);
        let mut dest = SymbolDb::new();

        dest.copy_partition_from(&source, 0, 17).unwrap();

        assert!(dest.intern_stacktrace(17, &[a, b]) == id);
        assert!(dest.copy_partition_from(&source, 0, 17).is_err());
    }
}

// === split-modules: generated submodules ===
mod drop_go_type_parameters;
mod empty_stacktrace_id;
mod function_rec;
mod go_shape_prefix;
mod line_rec;
mod location_rec;
mod mapping_rec;
mod mapping_symbolization;
mod partition;
mod raw_location;
mod remap_index;
mod symbol_db_type;
mod tree_node;

use drop_go_type_parameters::drop_go_type_parameters;
pub use empty_stacktrace_id::EMPTY_STACKTRACE_ID;
pub use function_rec::FunctionRec;
use go_shape_prefix::GO_SHAPE_PREFIX;
pub use line_rec::LineRec;
pub use location_rec::LocationRec;
pub use mapping_rec::MappingRec;
pub use mapping_symbolization::MappingSymbolization;
use partition::Partition;
pub use raw_location::RawLocation;
use remap_index::remap_index;
pub use symbol_db_type::SymbolDb;
use tree_node::TreeNode;
