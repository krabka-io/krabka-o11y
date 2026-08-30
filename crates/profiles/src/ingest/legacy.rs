//! Legacy `POST /ingest` door.

use std::{collections::BTreeMap, io::Cursor};

use krabka_blockstore::Labels;
use krabka_pprof::PprofProfile;
use krabka_units::{ByteSize, convert::ByteSizeExt as _};
use serde::Deserialize;

use crate::{error::ProfilesError, ingest::RawProfile};

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::mebibytes;

    use super::*;

    /// `read_tree_varint` decodes a base-128 varint and advances `pos` past
    /// exactly the bytes it consumed. Each case checks the value *and* the
    /// resulting offset, because a decoder that returns the right number but
    /// leaves the cursor in the wrong place corrupts every field after it.
    #[test]
    fn tree_varints_decode_and_advance_the_cursor() {
        let read = |bytes: &[u8]| {
            let mut pos = 0_usize;
            super::read_tree_varint(bytes, &mut pos, "field").map(|v| (v, pos))
        };

        check!(read(&[0x00]).unwrap() == (0, 1), "zero is one byte");
        check!(
            read(&[0x7f]).unwrap() == (127, 1),
            "the largest single byte"
        );
        check!(
            read(&[0x80, 0x01]).unwrap() == (128, 2),
            "the smallest two-byte value"
        );
        check!(
            read(&[0xff, 0x01]).unwrap() == (255, 2),
            "continuation bits are stripped"
        );
        check!(read(&[0xac, 0x02]).unwrap() == (300, 2), "low group first");
        check!(
            read(&[0xff, 0xff, 0xff, 0xff, 0x0f]).unwrap() == (u64::from(u32::MAX), 5),
            "a full u32 spans five bytes"
        );

        // A trailing byte after the terminator is left for the next field.
        let mut pos = 0_usize;
        check!(super::read_tree_varint(&[0x01, 0x02], &mut pos, "field").unwrap() == 1);
        check!(pos == 1, "the cursor stops at the terminator");

        // Running out of input names the field being read.
        let err = read(&[0x80]).unwrap_err().to_string();
        check!(err.contains("ended before field"), "got: {err}");
        let err = read(&[]).unwrap_err().to_string();
        check!(err.contains("ended before field"), "got: {err}");

        // Ten continuation bytes push the shift past what a u64 can hold.
        let err = read(&[0x80; 12]).unwrap_err().to_string();
        check!(err.contains("overflows u64"), "got: {err}");
    }

    /// Encodes one node of Pyroscope's binary tree format: a name, the value
    /// charged to the node itself, and how many children follow it.
    fn tree_node(name: &str, self_value: u64, children: u64) -> Vec<u8> {
        fn varint(mut value: u64, out: &mut Vec<u8>) {
            loop {
                let byte = u8::try_from(value & 0x7f).expect("seven bits fit a byte");
                value >>= 7;
                if value == 0 {
                    out.push(byte);
                    return;
                }
                out.push(byte | 0x80);
            }
        }
        let mut out = Vec::new();
        varint(name.len() as u64, &mut out);
        out.extend_from_slice(name.as_bytes());
        varint(self_value, &mut out);
        varint(children, &mut out);
        out
    }

    /// `tree_to_pprof` walks a pre-order node stream, carrying each node's
    /// path down to its children and charging self values to the path that
    /// reaches them.
    ///
    /// The fixture is an unnamed root over two children, the first of which
    /// has a child of its own, so the decoder has to resume the root's second
    /// child after descending. Both a named node with its own value and one
    /// whose value sits only in a descendant are represented.
    #[test]
    fn tree_nodes_decode_into_the_stacks_that_reach_them() {
        let mut body = Vec::new();
        body.extend(tree_node("", 0, 3));
        body.extend(tree_node("a", 10, 1));
        body.extend(tree_node("a1", 5, 0));
        body.extend(tree_node("b", 7, 0));
        // A named node worth nothing on its own. It must not become a sample:
        // only a positive self value earns one.
        body.extend(tree_node("c", 0, 0));

        let profile =
            super::tree_to_pprof("app", "bytes", &body, LegacyDecodeLimits::default()).unwrap();

        check!(profile.sample_types() == vec![("samples".to_string(), "bytes".to_string())]);

        let decoded: Vec<(Vec<&str>, &[i64])> = profile
            .samples()
            .iter()
            .map(|sample| (profile.stack_frames(sample), sample.value.as_slice()))
            .collect();
        check!(
            decoded
                == vec![
                    (vec!["a"], [10].as_slice()),
                    (vec!["a1", "a"], [5].as_slice()),
                    (vec!["b"], [7].as_slice()),
                ]
        );
    }

    /// A payload that stops immediately after a node name is short, not
    /// oversized: the name itself fits exactly, and it is the fields *after*
    /// it that are missing. The distinction decides which error the caller
    /// sees, so it is pinned here.
    #[test]
    fn a_tree_name_ending_at_the_payload_edge_reports_the_missing_field() {
        let mut body = Vec::new();
        body.extend(tree_node("", 0, 1));
        body.extend_from_slice(&[0x01, b'a']);

        let err = super::tree_to_pprof("app", "bytes", &body, LegacyDecodeLimits::default())
            .unwrap_err()
            .to_string();
        check!(err.contains("ended before node self value"), "got: {err}");
    }

    /// The node budget counts nodes, so a tree of exactly `max_nodes` is
    /// allowed and one more is not.
    #[test]
    fn the_tree_node_budget_admits_exactly_its_limit() {
        let mut body = Vec::new();
        body.extend(tree_node("", 0, 3));
        body.extend(tree_node("a", 10, 1));
        body.extend(tree_node("a1", 5, 0));
        body.extend(tree_node("b", 7, 0));
        body.extend(tree_node("c", 0, 0));

        let limits = |max_nodes| LegacyDecodeLimits {
            max_nodes,
            ..LegacyDecodeLimits::default()
        };
        check!(
            super::tree_to_pprof("app", "bytes", &body, limits(5)).is_ok(),
            "five nodes fit"
        );

        let err = super::tree_to_pprof("app", "bytes", &body, limits(4))
            .unwrap_err()
            .to_string();
        check!(err.contains("exceeds node budget"), "got: {err}");
    }

    /// A node may declare one more child than there are bytes left, because a
    /// child costs at least one byte only once its own fields are counted.
    /// The check is a cheap early reject, so at the boundary the payload has
    /// to fail later, on the missing bytes themselves, and say so.
    #[test]
    fn a_child_count_at_the_payload_edge_fails_on_the_missing_node() {
        let body = [0x00, 0x00, 0x01];

        let err = super::tree_to_pprof("app", "bytes", &body, LegacyDecodeLimits::default())
            .unwrap_err()
            .to_string();
        check!(err.contains("ended before node name length"), "got: {err}");

        // Two children with nothing left is over the line and is rejected up
        // front instead.
        let body = [0x00, 0x00, 0x02];
        let err = super::tree_to_pprof("app", "bytes", &body, LegacyDecodeLimits::default())
            .unwrap_err()
            .to_string();
        check!(
            err.contains("children length exceeds remaining payload"),
            "got: {err}"
        );
    }

    /// Encodes one node of Pyroscope's binary trie format: the suffix this
    /// node adds to its parent's key, the value charged to the whole key, and
    /// how many children follow.
    fn trie_node(suffix: &str, value: u64, children: u64) -> Vec<u8> {
        fn varint(mut value: u64, out: &mut Vec<u8>) {
            loop {
                let byte = u8::try_from(value & 0x7f).expect("seven bits fit a byte");
                value >>= 7;
                if value == 0 {
                    out.push(byte);
                    return;
                }
                out.push(byte | 0x80);
            }
        }
        let mut out = Vec::new();
        varint(suffix.len() as u64, &mut out);
        out.extend_from_slice(suffix.as_bytes());
        varint(value, &mut out);
        varint(children, &mut out);
        out
    }

    /// `trie_to_pprof` builds each node's key by appending its suffix to its
    /// parent's, then splits the finished key on ';' into frames.
    ///
    /// The fixture shares the prefix "main;" across two children, gives one of
    /// them a child of its own so the decoder has to unwind two levels, and
    /// includes a second top-level node so the synthetic forest root is
    /// exercised rather than a single tree.
    #[test]
    fn trie_nodes_extend_their_parents_key() {
        let mut body = Vec::new();
        body.extend(trie_node("main;", 0, 2));
        body.extend(trie_node("work", 7, 1));
        body.extend(trie_node(";inner", 4, 0));
        body.extend(trie_node("idle", 3, 0));
        body.extend(trie_node("other", 2, 0));

        let profile =
            super::trie_to_pprof("app", "bytes", &body, LegacyDecodeLimits::default()).unwrap();

        let decoded: Vec<(Vec<&str>, &[i64])> = profile
            .samples()
            .iter()
            .map(|sample| (profile.stack_frames(sample), sample.value.as_slice()))
            .collect();
        check!(
            decoded
                == vec![
                    (vec!["idle", "main"], [3].as_slice()),
                    (vec!["work", "main"], [7].as_slice()),
                    (vec!["inner", "work", "main"], [4].as_slice()),
                    (vec!["other"], [2].as_slice()),
                ]
        );
    }

    /// The same five-node trie as above, reused to pin each limit at its
    /// boundary rather than well inside it. Every one of these guards is an
    /// inequality, and an inequality tested only far from its edge is
    /// indistinguishable from the one next to it.
    #[test]
    fn the_trie_limits_admit_exactly_their_boundary() {
        let mut body = Vec::new();
        body.extend(trie_node("main;", 0, 2));
        body.extend(trie_node("work", 7, 1));
        body.extend(trie_node(";inner", 4, 0));
        body.extend(trie_node("idle", 3, 0));
        body.extend(trie_node("other", 2, 0));
        let decode = |limits| super::trie_to_pprof("app", "bytes", &body, limits);

        // Five nodes fit a budget of five, and not one of four.
        let nodes = |max_nodes| LegacyDecodeLimits {
            max_nodes,
            ..LegacyDecodeLimits::default()
        };
        check!(decode(nodes(5)).is_ok(), "five nodes fit a budget of five");
        let err = decode(nodes(4)).unwrap_err().to_string();
        check!(err.contains("exceeds node budget"), "got: {err}");

        // The deepest node sits under two frames, so a depth cap of two
        // rejects it and three admits it.
        let depth = |max_trie_depth| LegacyDecodeLimits {
            max_trie_depth,
            ..LegacyDecodeLimits::default()
        };
        check!(decode(depth(3)).is_ok(), "three levels fit a cap of three");
        let err = decode(depth(2)).unwrap_err().to_string();
        check!(err.contains("exceeds maximum depth"), "got: {err}");

        // Materialized keys total 43 bytes: the shared "main;" prefix is
        // copied into each descendant, which is the amplification the budget
        // exists to bound.
        let path = |n| LegacyDecodeLimits {
            max_path_bytes: krabka_units::bytes(n),
            ..LegacyDecodeLimits::default()
        };
        check!(
            decode(path(43)).is_ok(),
            "43 bytes of keys fit a budget of 43"
        );
        let err = decode(path(42)).unwrap_err().to_string();
        check!(err.contains("exceeds path-bytes budget"), "got: {err}");
    }

    /// A suffix that ends exactly at the payload edge is not oversized; the
    /// value and child count that should follow it are simply missing, and
    /// the error names that rather than the suffix.
    #[test]
    fn a_trie_suffix_ending_at_the_payload_edge_reports_the_missing_value() {
        let body = [0x04, b'm', b'a', b'i', b'n'];

        let err = super::trie_to_pprof("app", "bytes", &body, LegacyDecodeLimits::default())
            .unwrap_err()
            .to_string();
        check!(err.contains("ended before trie node value"), "got: {err}");
    }

    /// The (stack, value) pairs a decoded profile carries, root-first and
    /// sorted so a comparison does not depend on map order.
    fn frames_and_values(profile: &PprofProfile) -> Vec<(String, i64)> {
        // `stack_frames` returns leaf-first, the pprof convention; folded
        // input is written root-first.
        let mut out = profile
            .samples()
            .iter()
            .map(|sample| {
                let mut frames = profile
                    .stack_frames(sample)
                    .into_iter()
                    .map(str::to_string)
                    .collect::<Vec<_>>();
                frames.reverse();
                (frames.join(";"), sample.value[0])
            })
            .collect::<Vec<_>>();
        out.sort();
        out
    }

    /// Folded input has four rules and no test held any of them: repeats of a
    /// stack accumulate, the value is the *last* whitespace-separated field so
    /// frame names may contain spaces, empty frames between separators are
    /// dropped, and blank and `#` lines are skipped. Errors name the 1-based
    /// line.
    #[test]
    fn folded_stacks_accumulate_and_take_the_value_from_the_last_field() {
        let parse = |body: &str| super::folded_to_pprof("app", "count", body);

        // The same stack twice adds up; overwriting would leave 4.
        let profile = parse("main;work 3\nmain;work 4\n").expect("two folded lines");
        check!(frames_and_values(&profile) == vec![("main;work".to_string(), 7)]);

        // A comment and a blank are skipped; the value is the last field, so
        // "my func" survives as one frame; the empty frame is dropped.
        let profile = parse("# a comment\n\nmy func;;other 5\n").expect("one folded line");
        check!(frames_and_values(&profile) == vec![("my func;other".to_string(), 5)]);

        // The third line of the body is named as line 3, not 2.
        let err = parse("main 1\nmain 2\nnovalue\n").unwrap_err().to_string();
        check!(err.contains("folded line 3 missing value"), "got: {err}");
    }

    /// `lines` input is folded input without a value: each line counts once.
    /// It shares the comment, blank and empty-frame rules with
    /// [`folded_to_pprof`] and had no test for any of them, nor for a body
    /// that yields nothing being an error rather than an empty profile.
    #[test]
    fn lines_input_counts_one_per_line_and_skips_comments() {
        let parse = |body: &str| super::lines_to_pprof("app", "count", body);

        // The comment and the blank are skipped, the empty frame is dropped,
        // and both remaining lines fold into one stack counted twice.
        let profile = parse("# comment\n\nmain;;work\nmain;work\n").expect("two counted lines");
        check!(frames_and_values(&profile) == vec![("main;work".to_string(), 2)]);

        // Nothing countable is an error, not a profile with no samples.
        let err = parse("# only a comment\n").unwrap_err().to_string();
        check!(err.contains("lines profile has no samples"), "got: {err}");

        // The empty stack is on the third line, and the error says so.
        let err = parse("main\nmain\n;;\n").unwrap_err().to_string();
        check!(
            err.contains("lines profile line 3 has empty stack"),
            "got: {err}"
        );
    }

    /// Wraps `parts` as a multipart body with a fixed boundary.
    fn multipart_body(parts: &[(&str, &[u8])]) -> bytes::Bytes {
        let mut body = Vec::new();
        for (name, content) in parts {
            body.extend_from_slice(b"--test-boundary\r\n");
            body.extend_from_slice(
                format!("Content-Disposition: form-data; name=\"{name}\"\r\n").as_bytes(),
            );
            body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
            body.extend_from_slice(content);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(b"--test-boundary--\r\n");
        bytes::Bytes::from(body)
    }

    /// Each ingest format accepts its own part names and ignores the rest.
    /// The guards are all equality tests against the query's format, so a
    /// flipped one makes a format reject its own payload and accept every
    /// other format's -- which only shows up when the same part name is tried
    /// under a format that should ignore it.
    #[test]
    fn multipart_parts_are_accepted_only_under_their_own_format() {
        let folded = b"main;work 7\n".as_slice();
        let mut nested_nodes = Vec::new();
        nested_nodes.extend(tree_node("", 0, 1));
        nested_nodes.extend(tree_node("main", 7, 0));
        let shared_prefix = trie_node("main", 7, 0);
        let speedscope = br#"{
            "shared": { "frames": [{ "name": "main" }] },
            "profiles": [{
              "type": "sampled", "name": "cpu", "unit": "samples",
              "startValue": 0, "endValue": 10,
              "samples": [[0]], "weights": [2]
            }]
        }"#
        .as_slice();

        // (format, part name, payload, whether it should yield a profile)
        let cases: &[(&str, &str, &[u8], bool)] = &[
            ("groups", "profile", folded, true),
            ("groups", "groups", folded, true),
            ("groups", "folded", folded, true),
            ("groups", "tree", folded, false),
            ("lines", "profile", folded, true),
            ("lines", "folded", folded, true),
            ("tree", "profile", &nested_nodes, true),
            ("tree", "tree", &nested_nodes, true),
            ("tree", "trie", &nested_nodes, false),
            ("trie", "profile", &shared_prefix, true),
            ("trie", "trie", &shared_prefix, true),
            ("trie", "tree", &shared_prefix, false),
            ("speedscope", "profile", speedscope, true),
            ("speedscope", "speedscope", speedscope, true),
            ("speedscope", "tree", speedscope, false),
            // The jfr reader falls back to folded text for input that is not
            // a binary recording, which keeps this row cheap.
            ("jfr", "jfr", folded, true),
            ("jfr", "profile", folded, false),
        ];

        for (format, part, payload, expect_ok) in cases {
            let query = parse_ingest_query(&format!("name=app&format={format}")).unwrap();
            let result = futures::executor::block_on(super::decode_ingest_multipart_with_limits(
                &query,
                "multipart/form-data; boundary=test-boundary",
                multipart_body(&[(part, payload)]),
                mebibytes(1),
                LegacyDecodeLimits::default(),
            ));
            check!(
                result.is_ok() == *expect_ok,
                "format={format} part={part} gave {result:?}"
            );
        }
    }

    /// The per-part size cap rejects what exceeds it, so a part of exactly
    /// the limit is still accepted.
    #[test]
    fn a_multipart_part_of_exactly_the_limit_is_accepted() {
        let folded = b"main;work 7\n".as_slice();
        let query = parse_ingest_query("name=app&format=groups").unwrap();
        let decode = |limit| {
            futures::executor::block_on(super::decode_ingest_multipart_with_limits(
                &query,
                "multipart/form-data; boundary=test-boundary",
                multipart_body(&[("profile", folded)]),
                krabka_units::bytes(limit),
                LegacyDecodeLimits::default(),
            ))
        };

        let len = u32::try_from(folded.len()).expect("fixture fits a u32");
        check!(decode(len).is_ok(), "a part exactly at the limit fits");
        let err = decode(len - 1).unwrap_err();
        check!(
            matches!(err, ProfilesError::TooLarge { .. }),
            "got: {err:?}"
        );
    }

    /// A part with no name is not a profile. Nothing downstream distinguishes
    /// an unnamed part from one named "profile" except this lookup, so an
    /// anonymous payload must be ignored rather than adopted.
    #[test]
    fn an_unnamed_multipart_part_is_ignored() {
        let query = parse_ingest_query("name=app&format=groups").unwrap();
        let mut body = Vec::new();
        body.extend_from_slice(b"--test-boundary\r\n");
        body.extend_from_slice(b"Content-Disposition: form-data\r\n");
        body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
        body.extend_from_slice(b"main;work 7\n");
        body.extend_from_slice(b"\r\n--test-boundary--\r\n");

        let result = futures::executor::block_on(super::decode_ingest_multipart_with_limits(
            &query,
            "multipart/form-data; boundary=test-boundary",
            bytes::Bytes::from(body),
            mebibytes(1),
            LegacyDecodeLimits::default(),
        ));
        check!(
            result.is_err(),
            "an unnamed part must not be read as the profile"
        );
    }

    /// A "labels" part carries extra labels, but only for jfr uploads. Under
    /// any other format the part is not a labels document and must be left
    /// alone, so the guard is checked from both sides.
    #[test]
    fn a_labels_part_is_read_only_for_jfr_uploads() {
        let folded = b"main;work 7\n".as_slice();
        let labels = br#"{"service_name":"payments"}"#.as_slice();
        let decode = |format: &str, profile_part: &str| {
            let query = parse_ingest_query(&format!("name=app&format={format}")).unwrap();
            futures::executor::block_on(super::decode_ingest_multipart_with_limits(
                &query,
                "multipart/form-data; boundary=test-boundary",
                multipart_body(&[("labels", labels), (profile_part, folded)]),
                mebibytes(1),
                LegacyDecodeLimits::default(),
            ))
        };

        let raw = decode("jfr", "jfr").unwrap();
        check!(raw.labels.get("service_name") == Some("payments"));

        // The same part under a format that has no labels concept.
        let raw = decode("groups", "profile").unwrap();
        check!(
            raw.labels.get("service_name") == None,
            "labels: {:?}",
            raw.labels
        );
    }

    /// Ingest timestamps arrive as either seconds or milliseconds and are
    /// told apart by magnitude, so the cutoff itself is the interesting part.
    /// `i64::MIN` is included because it is the one input whose magnitude
    /// cannot be taken as a signed value.
    #[test]
    fn ingest_times_below_the_cutoff_are_read_as_seconds() {
        let parse = super::parse_unix_time_ms;

        check!(parse("0").unwrap() == 0);
        check!(parse("1").unwrap() == 1_000, "seconds scale up");
        check!(
            parse("  7  ").unwrap() == 7_000,
            "surrounding space is trimmed"
        );
        check!(parse("-1").unwrap() == -1_000, "negative seconds scale too");
        check!(
            parse("9999999999").unwrap() == 9_999_999_999_000,
            "the largest value still read as seconds"
        );
        check!(
            parse("10000000000").unwrap() == 10_000_000_000,
            "the cutoff itself is already milliseconds"
        );
        check!(
            parse("-10000000000").unwrap() == -10_000_000_000,
            "the cutoff is on magnitude, not sign"
        );
        check!(parse("9223372036854775807").unwrap() == i64::MAX);
        check!(parse("-9223372036854775808").unwrap() == i64::MIN);

        check!(parse("").is_err(), "an empty value is not a time");
        check!(parse("later").is_err(), "a word is not a time");
    }

    /// `urldecode` expands percent escapes and '+' in ingest query strings.
    /// Anything it cannot expand is passed through as written, so a malformed
    /// escape neither disappears nor takes the characters after it with it.
    #[test]
    fn urldecode_expands_escapes_and_passes_through_the_rest() {
        let decode = super::urldecode;

        check!(decode("plain") == "plain", "ordinary text is untouched");
        check!(decode("") == "", "an empty string stays empty");
        check!(decode("a+b") == "a b", "plus is a space");
        check!(decode("a%20b") == "a b", "an escape is expanded");
        check!(decode("a%2Fb") == "a/b", "uppercase hex");
        check!(decode("a%2fb") == "a/b", "lowercase hex");
        check!(decode("%41%42") == "AB", "back to back escapes");
        // Percent is not self-escaping here: "%%" is simply an escape whose
        // first digit is not hex, so both characters survive.
        check!(
            decode("100%%") == "100%%",
            "a doubled percent is not one literal"
        );

        // Malformed escapes keep every character they consumed.
        check!(
            decode("a%zz") == "a%zz",
            "unparseable hex is left as written"
        );
        check!(
            decode("a%2") == "a%2",
            "an escape cut short keeps its digit"
        );
        check!(decode("a%") == "a%", "a trailing percent stands alone");
        check!(decode("a%2z") == "a%2z", "a bad second digit is kept too");
    }

    /// A jfr labels part is a flat JSON object. Scalars are stringified so
    /// that a label written as a number and one written as a string arrive
    /// the same way; anything with structure is rejected rather than
    /// flattened into something meaningless.
    #[test]
    fn jfr_labels_stringify_scalars_and_reject_structure() {
        let parse = |raw: &str| super::parse_labels_part(raw.as_bytes());

        check!(parse("").unwrap() == vec![], "an absent part is no labels");
        check!(
            parse("{}").unwrap() == vec![],
            "an empty object is no labels"
        );

        let labels =
            parse(r#"{"text":"a","int":7,"float":1.5,"yes":true,"no":false,"nothing":null}"#)
                .unwrap();
        // Document order is kept rather than sorted, so a caller reading the
        // first label gets the first one written.
        check!(
            labels
                == vec![
                    ("text".to_string(), "a".to_string()),
                    ("int".to_string(), "7".to_string()),
                    ("float".to_string(), "1.5".to_string()),
                    ("yes".to_string(), "true".to_string()),
                    ("no".to_string(), "false".to_string()),
                    ("nothing".to_string(), String::new()),
                ]
        );

        let err = parse(r#"{"list":[1,2]}"#).unwrap_err().to_string();
        check!(err.contains("`list` must be a scalar"), "got: {err}");
        let err = parse(r#"{"nested":{"a":1}}"#).unwrap_err().to_string();
        check!(err.contains("`nested` must be a scalar"), "got: {err}");
        let err = parse("[1,2]").unwrap_err().to_string();
        check!(err.contains("must be a JSON object"), "got: {err}");
        let err = parse("not json").unwrap_err().to_string();
        check!(err.contains("is not JSON"), "got: {err}");
    }

    #[test]
    fn parse_query_extracts_name_labels_format() {
        let q =
            parse_ingest_query("name=myapp{env=\"prod\",team=\"core\"}&format=pprof&sampleRate=97")
                .unwrap();

        check!(q.name == "myapp");
        check!(q.labels.contains(&("env".to_string(), "prod".to_string())));
        assert!(matches!(q.format, IngestFormat::Pprof));
        check!(q.sample_rate == 97);
    }

    #[test]
    fn sample_rate_is_validated_and_sets_raw_profile_period() {
        assert!(parse_ingest_query("name=app&sampleRate=0").is_err());
        assert!(parse_ingest_query("name=app&sampleRate=nope").is_err());

        let profile = stacks_to_pprof(
            "app",
            "samples",
            "count",
            BTreeMap::from([(vec![("root".to_string(), 0)], 1)]),
        );
        let profile = apply_query_sample_rate(profile, 250).into_inner();
        assert!(profile.period == 4_000_000);
    }

    #[test]
    fn unknown_format_defaults_to_groups() {
        let q = parse_ingest_query("name=app").unwrap();

        assert!(matches!(q.format, IngestFormat::Groups));
    }

    #[tokio::test]
    async fn decode_multipart_pprof_profile_part() {
        let query =
            parse_ingest_query("name=myapp{env=\"prod\"}&format=pprof&sampleRate=7").unwrap();
        let boundary = "test-boundary";
        let pprof = crate::wire::test_fixtures::cpu_profile_pprof_bytes();
        let original_period = PprofProfile::decode(&pprof).unwrap().inner().period;
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"profile\"\r\n");
        body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
        body.extend_from_slice(&pprof);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

        let raw = decode_ingest_multipart(
            &query,
            &format!("multipart/form-data; boundary={boundary}"),
            bytes::Bytes::from(body),
            mebibytes(1),
        )
        .await
        .unwrap();

        check!(raw.labels.get("__name__") == Some("myapp"));
        check!(raw.labels.get("env") == Some("prod"));
        check!(raw.profile.sample_types()[0].0 == "cpu");
        check!(raw.profile.inner().period == original_period);
    }

    #[tokio::test]
    async fn decode_multipart_pprof_applies_sample_type_config() {
        let query = parse_ingest_query("name=myapp&format=pprof").unwrap();
        let boundary = "test-boundary";
        let pprof = crate::wire::test_fixtures::cpu_profile_pprof_bytes();
        let config = r#"{"units":"nanoseconds","display-name":"wall","aggregation":"sum","cumulative":false,"sampled":true}"#;
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"sample_type_config\"\r\n");
        body.extend_from_slice(b"Content-Type: application/json\r\n\r\n");
        body.extend_from_slice(config.as_bytes());
        body.extend_from_slice(format!("\r\n--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"profile\"\r\n");
        body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
        body.extend_from_slice(&pprof);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

        let raw = decode_ingest_multipart(
            &query,
            &format!("multipart/form-data; boundary={boundary}"),
            bytes::Bytes::from(body),
            mebibytes(1),
        )
        .await
        .unwrap();

        assert!(raw.profile.sample_types()[0] == ("wall".to_string(), "nanoseconds".to_string()));
        assert!(
            raw.profile.period_type_strings() == ("wall".to_string(), "nanoseconds".to_string())
        );
        let split = crate::ingest::split_sample_types(&raw).unwrap();
        assert!(split[0].profile_type == "myapp:wall:nanoseconds:wall:nanoseconds:delta");
    }

    #[test]
    fn sample_type_config_rejects_semantics_it_cannot_apply() {
        let average = parse_sample_type_config(br#"{"aggregation":"average"}"#);
        assert!(matches!(average, Err(ProfilesError::Invalid(_))));
        let unsampled = parse_sample_type_config(br#"{"sampled":false}"#);
        assert!(matches!(unsampled, Err(ProfilesError::Invalid(_))));
        assert!(parse_sample_type_config(br#"{"aggregation":"sum","sampled":true}"#).is_ok());
    }

    #[tokio::test]
    async fn decode_multipart_folded_groups_profile_part() {
        let query = parse_ingest_query("name=myapp{env=\"prod\"}").unwrap();
        let boundary = "test-boundary";
        let folded = "main;work 7\nmain;idle 3\n";
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"profile\"\r\n");
        body.extend_from_slice(b"Content-Type: text/plain\r\n\r\n");
        body.extend_from_slice(folded.as_bytes());
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

        let raw = decode_ingest_multipart(
            &query,
            &format!("multipart/form-data; boundary={boundary}"),
            bytes::Bytes::from(body),
            mebibytes(1),
        )
        .await
        .unwrap();

        check!(raw.labels.get("__name__") == Some("myapp"));
        check!(raw.profile.sample_types()[0] == ("samples".to_string(), "count".to_string()));
        check!(raw.profile.samples().len() == 2);
    }

    #[tokio::test]
    async fn decode_multipart_folded_groups_applies_query_units() {
        let query = parse_ingest_query("name=myapp&units=bytes").unwrap();
        let boundary = "test-boundary";
        let folded = "main;work 7\n";
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"profile\"\r\n");
        body.extend_from_slice(b"Content-Type: text/plain\r\n\r\n");
        body.extend_from_slice(folded.as_bytes());
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

        let raw = decode_ingest_multipart(
            &query,
            &format!("multipart/form-data; boundary={boundary}"),
            bytes::Bytes::from(body),
            mebibytes(1),
        )
        .await
        .unwrap();

        assert!(raw.profile.sample_types()[0] == ("samples".to_string(), "bytes".to_string()));
    }

    #[tokio::test]
    async fn decode_plain_lines_counts_repeated_stack_lines() {
        let query =
            parse_ingest_query("name=myapp&format=lines&units=samples&sampleRate=250").unwrap();
        let body = "main;work\nmain;work\nmain;idle\nmain;work\n";

        let raw = decode_ingest_body(
            &query,
            Some("text/plain"),
            bytes::Bytes::from(body),
            mebibytes(1),
        )
        .await
        .unwrap();

        let mut values = raw
            .profile
            .inner()
            .sample
            .iter()
            .map(|sample| sample.value[0])
            .collect::<Vec<_>>();
        values.sort_unstable();

        assert!(raw.profile.sample_types()[0] == ("samples".to_string(), "samples".to_string()));
        assert!(raw.profile.inner().period == 4_000_000);
        assert!(values == vec![1, 3]);
    }

    #[tokio::test]
    async fn decode_plain_speedscope_sampled_profile_uses_shared_frames_and_weights() {
        let query = parse_ingest_query("name=myapp&format=speedscope&units=samples").unwrap();
        let body = r#"{
          "$schema": "https://www.speedscope.app/file-format-schema.json",
          "shared": {
            "frames": [
              { "name": "main" },
              { "name": "work" },
              { "name": "idle" }
            ]
          },
          "profiles": [{
            "type": "sampled",
            "name": "cpu",
            "unit": "samples",
            "startValue": 0,
            "endValue": 10,
            "samples": [[0, 1], [0, 1], [0, 2]],
            "weights": [2, 3, 4]
          }]
        }"#;

        let raw = decode_ingest_body(
            &query,
            Some("application/json"),
            bytes::Bytes::from(body),
            mebibytes(1),
        )
        .await
        .unwrap();

        let mut values = raw
            .profile
            .inner()
            .sample
            .iter()
            .map(|sample| sample.value[0])
            .collect::<Vec<_>>();
        values.sort_unstable();

        assert!(raw.profile.sample_types()[0] == ("samples".to_string(), "samples".to_string()));
        assert!(values == vec![4, 5]);
    }

    #[tokio::test]
    async fn decode_plain_tree_format_payload_uses_serialized_tree_nodes() {
        let query = parse_ingest_query("name=myapp&format=tree&units=samples").unwrap();
        let body =
            bytes::Bytes::from_static(b"\x00\x00\x01\x01a\x00\x02\x01b\x01\x00\x01c\x02\x00");

        let raw = decode_ingest_body(&query, Some("application/octet-stream"), body, mebibytes(1))
            .await
            .unwrap();

        let mut values = raw
            .profile
            .inner()
            .sample
            .iter()
            .map(|sample| sample.value[0])
            .collect::<Vec<_>>();
        values.sort_unstable();
        let functions = raw
            .profile
            .inner()
            .function
            .iter()
            .filter_map(|function| raw.profile.string(function.name))
            .collect::<Vec<_>>();

        check!(raw.profile.sample_types()[0] == ("samples".to_string(), "samples".to_string()));
        check!(values == vec![1, 2]);
        for function in ["a", "b", "c"] {
            check!(functions.contains(&function));
        }
    }

    #[tokio::test]
    async fn decode_plain_trie_format_payload_uses_serialized_folded_stack_trie() {
        let query = parse_ingest_query("name=myapp&format=trie&units=samples").unwrap();
        let body =
            bytes::Bytes::from_static(b"\x00\x00\x01\x02a;\x00\x02\x01b\x01\x00\x01c\x02\x00");

        let raw = decode_ingest_body(&query, Some("application/octet-stream"), body, mebibytes(1))
            .await
            .unwrap();

        let mut values = raw
            .profile
            .inner()
            .sample
            .iter()
            .map(|sample| sample.value[0])
            .collect::<Vec<_>>();
        values.sort_unstable();
        let functions = raw
            .profile
            .inner()
            .function
            .iter()
            .filter_map(|function| raw.profile.string(function.name))
            .collect::<Vec<_>>();

        check!(raw.profile.sample_types()[0] == ("samples".to_string(), "samples".to_string()));
        check!(values == vec![1, 2]);
        for function in ["a", "b", "c"] {
            check!(functions.contains(&function));
        }
    }

    #[tokio::test]
    async fn decode_multipart_folded_groups_uses_until_as_profile_time() {
        let query =
            parse_ingest_query("name=myapp&from=1699999999000&until=1700000000000").unwrap();
        let boundary = "test-boundary";
        let folded = "main;work 7\n";
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"profile\"\r\n");
        body.extend_from_slice(b"Content-Type: text/plain\r\n\r\n");
        body.extend_from_slice(folded.as_bytes());
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

        let raw = decode_ingest_multipart(
            &query,
            &format!("multipart/form-data; boundary={boundary}"),
            bytes::Bytes::from(body),
            mebibytes(1),
        )
        .await
        .unwrap();

        assert!(raw.profile.inner().time_nanos == 1_700_000_000_000_000_000);
    }

    #[tokio::test]
    async fn decode_multipart_jfr_part_with_labels_as_folded_stacks() {
        let query = parse_ingest_query("name=myapp&format=jfr").unwrap();
        let boundary = "test-boundary";
        let folded =
            "java.lang.Thread.run;app.Worker.loop 11\njava.lang.Thread.run;app.Worker.idle 2\n";
        let labels = r#"{"service_name":"payments","region":"us-east"}"#;
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"labels\"\r\n");
        body.extend_from_slice(b"Content-Type: application/json\r\n\r\n");
        body.extend_from_slice(labels.as_bytes());
        body.extend_from_slice(format!("\r\n--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            b"Content-Disposition: form-data; name=\"jfr\"; filename=\"profile.jfr\"\r\n",
        );
        body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
        body.extend_from_slice(folded.as_bytes());
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

        let raw = decode_ingest_multipart(
            &query,
            &format!("multipart/form-data; boundary={boundary}"),
            bytes::Bytes::from(body),
            mebibytes(1),
        )
        .await
        .unwrap();

        for (name, value) in [
            ("__name__", "myapp"),
            ("service_name", "payments"),
            ("region", "us-east"),
        ] {
            check!(raw.labels.get(name) == Some(value));
        }
        check!(raw.profile.sample_types()[0] == ("samples".to_string(), "count".to_string()));
        check!(raw.profile.samples().len() == 2);
    }

    #[tokio::test]
    async fn decode_multipart_jfr_binary_execution_samples() {
        let query = parse_ingest_query("name=myapp&format=jfr").unwrap();
        let boundary = "test-boundary";
        let jfr = include_bytes!("../../tests/fixtures/profiler-wall.jfr");
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            b"Content-Disposition: form-data; name=\"jfr\"; filename=\"profile.jfr\"\r\n",
        );
        body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
        body.extend_from_slice(jfr);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

        let raw = decode_ingest_multipart(
            &query,
            &format!("multipart/form-data; boundary={boundary}"),
            bytes::Bytes::from(body),
            mebibytes(1),
        )
        .await
        .unwrap();

        assert!(raw.profile.sample_types()[0] == ("wall".to_string(), "nanoseconds".to_string()));
        assert!(!raw.profile.samples().is_empty());
        let functions = raw
            .profile
            .inner()
            .function
            .iter()
            .filter_map(|function| raw.profile.string(function.name))
            .collect::<Vec<_>>();
        assert!(
            functions
                .iter()
                .any(|function| function.contains("CompileBroker::compiler_thread_loop"))
        );
    }

    /// LEB128 varint encoder that mirrors [`read_tree_varint`]. The
    /// amplification tests below use it to craft adversarial tree and trie
    /// payloads.
    fn put_tree_varint(out: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = u8::try_from(value & 0x7f).unwrap();
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    #[test]
    fn tree_decoder_rejects_path_bytes_amplification() {
        // A single child node with a very long name that then declares a large
        // child count: the decoder would clone the long path once per declared
        // child (`repeat_n(path, children_len)`), amplifying memory far beyond
        // the ~17 KB input. The cumulative path-bytes budget must reject it as
        // a Decode error instead of OOMing.
        let long = 9_000_usize;
        let children = 8_000_u64;
        let mut body = Vec::new();
        // Root: empty name, no self value, exactly one child.
        put_tree_varint(&mut body, 0);
        put_tree_varint(&mut body, 0);
        put_tree_varint(&mut body, 1);
        // Child: long name, no self value, `children` declared children.
        put_tree_varint(&mut body, long as u64);
        body.extend(std::iter::repeat_n(b'a', long));
        put_tree_varint(&mut body, 0);
        put_tree_varint(&mut body, children);
        // Filler bytes so the per-node remaining-payload guard passes and the
        // path-bytes guard (not the cheap structural one) is what fires.
        body.extend(std::iter::repeat_n(
            0_u8,
            usize::try_from(children).unwrap(),
        ));

        let limits = LegacyDecodeLimits::default();
        let err = tree_to_pprof("app", "samples", &body, limits).unwrap_err();
        assert!(matches!(err, ProfilesError::Decode(_)));

        // Sanity: a normal small tree still decodes successfully.
        let ok = b"\x00\x00\x01\x01a\x00\x02\x01b\x01\x00\x01c\x02\x00";
        assert!(tree_to_pprof("app", "samples", ok, limits).is_ok());
    }

    #[test]
    fn tree_decoder_uses_configured_node_budget() {
        let body = b"\x00\x00\x01\x01a\x01\x00";
        let limits = LegacyDecodeLimits {
            max_nodes: 1,
            ..LegacyDecodeLimits::default()
        };

        assert!(tree_to_pprof("app", "samples", body, limits).is_err());
        assert!(tree_to_pprof("app", "samples", body, LegacyDecodeLimits::default()).is_ok());
    }

    #[test]
    fn trie_decoder_rejects_deep_payload_past_depth_cap() {
        // A linear chain of single-child nodes deeper than the configured cap. The
        // old recursive `parse_trie_node` would recurse once per level and blow
        // the native stack; the explicit work-stack must reject past the cap.
        let limits = LegacyDecodeLimits {
            max_trie_depth: 64,
            ..LegacyDecodeLimits::default()
        };
        let depth = limits.max_trie_depth + 16;
        let mut body = Vec::new();
        for _ in 0..depth - 1 {
            // suffix "a", value 0, one child.
            put_tree_varint(&mut body, 1);
            body.push(b'a');
            put_tree_varint(&mut body, 0);
            put_tree_varint(&mut body, 1);
        }
        // Leaf: suffix "a", value 1, no children.
        put_tree_varint(&mut body, 1);
        body.push(b'a');
        put_tree_varint(&mut body, 1);
        put_tree_varint(&mut body, 0);

        let err = trie_to_pprof("app", "samples", &body, limits).unwrap_err();
        assert!(matches!(err, ProfilesError::Decode(_)));

        // Sanity: a chain comfortably under the cap still decodes.
        let shallow_depth = 32_usize;
        let mut shallow = Vec::new();
        for _ in 0..shallow_depth - 1 {
            put_tree_varint(&mut shallow, 1);
            shallow.push(b'a');
            put_tree_varint(&mut shallow, 0);
            put_tree_varint(&mut shallow, 1);
        }
        put_tree_varint(&mut shallow, 1);
        shallow.push(b'a');
        put_tree_varint(&mut shallow, 1);
        put_tree_varint(&mut shallow, 0);
        assert!(trie_to_pprof("app", "samples", &shallow, limits).is_ok());

        // Sanity: the canonical small trie payload still decodes.
        let ok = b"\x00\x00\x01\x02a;\x00\x02\x01b\x01\x00\x01c\x02\x00";
        assert!(trie_to_pprof("app", "samples", ok, limits).is_ok());
    }
}

// === split-modules: generated submodules ===
mod apply_query_sample_rate;
mod apply_query_time;
mod apply_sample_type_config;
mod binary_jfr_to_pprof;
mod decode_ingest_body;
mod decode_ingest_body_with_limits;
mod decode_ingest_multipart;
mod decode_ingest_multipart_with_limits;
mod folded_to_pprof;
mod ingest_format;
mod ingest_query;
mod intern_profile_string;
mod intern_string;
mod jfr_method_name;
mod jfr_to_pprof;
mod legacy_decode_limits;
mod lines_to_pprof;
mod parse_ingest_query;
mod parse_labels_part;
mod parse_sample_type_config;
mod parse_unix_time_ms;
mod query_labels;
mod read_tree_varint;
mod sample_type_config;
mod speedscope_to_pprof;
mod split_app_labels;
mod stacks_to_pprof;
mod tree_to_pprof;
mod trie_frame;
mod trie_to_pprof;
mod urldecode;

use apply_query_sample_rate::apply_query_sample_rate;
use apply_query_time::apply_query_time;
use apply_sample_type_config::apply_sample_type_config;
use binary_jfr_to_pprof::binary_jfr_to_pprof;
pub use decode_ingest_body::decode_ingest_body;
pub use decode_ingest_body_with_limits::decode_ingest_body_with_limits;
pub use decode_ingest_multipart::decode_ingest_multipart;
pub use decode_ingest_multipart_with_limits::decode_ingest_multipart_with_limits;
use folded_to_pprof::folded_to_pprof;
pub use ingest_format::IngestFormat;
pub use ingest_query::IngestQuery;
use intern_profile_string::intern_profile_string;
use intern_string::intern_string;
use jfr_method_name::jfr_method_name;
use jfr_to_pprof::jfr_to_pprof;
pub use legacy_decode_limits::LegacyDecodeLimits;
use lines_to_pprof::lines_to_pprof;
pub use parse_ingest_query::parse_ingest_query;
use parse_labels_part::parse_labels_part;
use parse_sample_type_config::parse_sample_type_config;
use parse_unix_time_ms::parse_unix_time_ms;
use query_labels::query_labels;
use read_tree_varint::read_tree_varint;
use sample_type_config::SampleTypeConfig;
use speedscope_to_pprof::speedscope_to_pprof;
use split_app_labels::split_app_labels;
use stacks_to_pprof::stacks_to_pprof;
use tree_to_pprof::tree_to_pprof;
use trie_frame::TrieFrame;
use trie_to_pprof::trie_to_pprof;
use urldecode::urldecode;
