use assert2::assert;
use krabka_pprof::proto::{Mapping, MappingSymbolization};
use prost::Message;

#[test]
fn mapping_preserves_wire_tags_for_every_symbolization_flag_combination() {
    for bits in 0_u8..16 {
        let flags = (bits & 1 != 0, bits & 2 != 0, bits & 4 != 0, bits & 8 != 0);
        let expected = Mapping {
            id: 1,
            memory_start: 2,
            memory_limit: 3,
            file_offset: 4,
            filename: 5,
            build_id: 6,
            symbolization: MappingSymbolization::from_parts(flags),
        };
        let mut wire = vec![8, 1, 16, 2, 24, 3, 32, 4, 40, 5, 48, 6];
        for (bit, key) in [(1, 56), (2, 64), (4, 72), (8, 80)] {
            if bits & bit != 0 {
                wire.extend([key, 1]);
            }
        }

        assert!(expected.encode_to_vec() == wire, "flags {bits}");
        assert!(expected.encoded_len() == wire.len(), "flags {bits}");
        assert!(Mapping::decode(wire.as_slice()).unwrap() == expected);
    }
}

#[test]
fn mapping_uses_last_boolean_value_and_skips_unknown_fields() {
    let wire = [
        56, 1, 56, 0, // functions: true, then false
        64, 0, 64, 1, // filenames: false, then true
        72, 1, 80, 1, // line numbers and inline frames
        88, 42, // unknown varint field
        90, 3, 7, 8, 9, // unknown length-delimited field
    ];
    let expected = Mapping {
        symbolization: MappingSymbolization::from_parts((false, true, true, true)),
        ..Mapping::default()
    };
    let mut decoded = Mapping::decode(wire.as_slice()).unwrap();
    assert!(decoded == expected);
    decoded.clear();
    assert!(decoded == Mapping::default());
    assert!(decoded.encode_to_vec().is_empty());
}
