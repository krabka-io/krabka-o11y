// Supported functions registered by pinned Loki pkg/logql/log/fmt.go.
// Expected values are independent of the candidate renderer.
pub struct TemplateFunctionCase {
    pub name: &'static str,
    pub expression: &'static str,
    pub expected: &'static str,
}

pub const CASES: &[TemplateFunctionCase] = &[
    TemplateFunctionCase {
        name: "b64enc",
        expression: r#"b64enc "abc""#,
        expected: "YWJj",
    },
    TemplateFunctionCase {
        name: "b64dec",
        expression: r#"b64dec "YWJj""#,
        expected: "abc",
    },
    TemplateFunctionCase {
        name: "lower",
        expression: r#"lower "AbC""#,
        expected: "abc",
    },
    TemplateFunctionCase {
        name: "upper",
        expression: r#"upper "aBc""#,
        expected: "ABC",
    },
    TemplateFunctionCase {
        name: "title",
        expression: r#"title "hello world""#,
        expected: "Hello World",
    },
    TemplateFunctionCase {
        name: "trunc",
        expression: r#"trunc -3 "abcdef""#,
        expected: "def",
    },
    TemplateFunctionCase {
        name: "substr",
        expression: r#"substr 1 4 "abcdef""#,
        expected: "bcd",
    },
    TemplateFunctionCase {
        name: "contains",
        expression: r#"contains "bc" "abcd""#,
        expected: "true",
    },
    TemplateFunctionCase {
        name: "hasPrefix",
        expression: r#"hasPrefix "ab" "abcd""#,
        expected: "true",
    },
    TemplateFunctionCase {
        name: "hasSuffix",
        expression: r#"hasSuffix "cd" "abcd""#,
        expected: "true",
    },
    TemplateFunctionCase {
        name: "indent",
        expression: r#"indent 2 "a\nb""#,
        expected: "  a\n  b",
    },
    TemplateFunctionCase {
        name: "nindent",
        expression: r#"nindent 2 "a\nb""#,
        expected: "\n  a\n  b",
    },
    TemplateFunctionCase {
        name: "replace",
        expression: r#"replace "a" "x" "aba""#,
        expected: "xbx",
    },
    TemplateFunctionCase {
        name: "repeat",
        expression: r#"repeat 3 "ab""#,
        expected: "ababab",
    },
    TemplateFunctionCase {
        name: "trim",
        expression: r#"trim "  abc  ""#,
        expected: "abc",
    },
    TemplateFunctionCase {
        name: "trimAll",
        expression: r#"trimAll "/" "//abc//""#,
        expected: "abc",
    },
    TemplateFunctionCase {
        name: "trimSuffix",
        expression: r#"trimSuffix "bc" "abc""#,
        expected: "a",
    },
    TemplateFunctionCase {
        name: "trimPrefix",
        expression: r#"trimPrefix "ab" "abc""#,
        expected: "c",
    },
    TemplateFunctionCase {
        name: "int",
        expression: r#"int "42""#,
        expected: "42",
    },
    TemplateFunctionCase {
        name: "float64",
        expression: r#"float64 "2.5""#,
        expected: "2.5",
    },
    TemplateFunctionCase {
        name: "add",
        expression: "add 7 3",
        expected: "10",
    },
    TemplateFunctionCase {
        name: "sub",
        expression: "sub 7 3",
        expected: "4",
    },
    TemplateFunctionCase {
        name: "mul",
        expression: "mul 7 3",
        expected: "21",
    },
    TemplateFunctionCase {
        name: "div",
        expression: "div 7 3",
        expected: "2",
    },
    TemplateFunctionCase {
        name: "mod",
        expression: "mod 7 3",
        expected: "1",
    },
    TemplateFunctionCase {
        name: "addf",
        expression: "addf 7.5 3.25",
        expected: "10.75",
    },
    TemplateFunctionCase {
        name: "subf",
        expression: "subf 7.5 3.25 1",
        expected: "3.25",
    },
    TemplateFunctionCase {
        name: "mulf",
        expression: "mulf 2.5 4",
        expected: "10",
    },
    TemplateFunctionCase {
        name: "divf",
        expression: "divf 10 2 4",
        expected: "1.25",
    },
    TemplateFunctionCase {
        name: "max",
        expression: "max 1 7 3",
        expected: "7",
    },
    TemplateFunctionCase {
        name: "min",
        expression: "min 1 7 3",
        expected: "1",
    },
    TemplateFunctionCase {
        name: "maxf",
        expression: "maxf 1.5 7.5 3.5",
        expected: "7.5",
    },
    TemplateFunctionCase {
        name: "minf",
        expression: "minf 1.5 7.5 3.5",
        expected: "1.5",
    },
    TemplateFunctionCase {
        name: "ceil",
        expression: "ceil 2.1",
        expected: "3",
    },
    TemplateFunctionCase {
        name: "floor",
        expression: "floor 2.9",
        expected: "2",
    },
    TemplateFunctionCase {
        name: "round",
        expression: "round 1.235 2",
        expected: "1.24",
    },
    TemplateFunctionCase {
        name: "fromJson",
        expression: r#"fromJson "{\"a\":[1,2]}""#,
        expected: "map[a:[1 2]]",
    },
    TemplateFunctionCase {
        name: "date",
        expression: r#"date "2006-01-02" (toDate "2006-01-02" "2024-01-02")"#,
        expected: "2024-01-02",
    },
    TemplateFunctionCase {
        name: "toDate",
        expression: r#"toDate "2006-01-02" "2024-01-02" | unixEpoch"#,
        expected: "1704153600",
    },
    TemplateFunctionCase {
        name: "unixEpoch",
        expression: r#"unixEpoch (toDate "2006-01-02" "2024-01-02")"#,
        expected: "1704153600",
    },
    TemplateFunctionCase {
        name: "default",
        expression: r#"default "fallback" (fromJson "[]")"#,
        expected: "fallback",
    },
    TemplateFunctionCase {
        name: "regexReplaceAll",
        expression: r#"regexReplaceAll "(a)b" "abc" "${1}x""#,
        expected: "axc",
    },
    TemplateFunctionCase {
        name: "regexReplaceAllLiteral",
        expression: r#"regexReplaceAllLiteral "(a)b" "abc" "${1}x""#,
        expected: "${1}xc",
    },
    TemplateFunctionCase {
        name: "count",
        expression: r#"count "a" "banana""#,
        expected: "3",
    },
    TemplateFunctionCase {
        name: "urldecode",
        expression: r#"urldecode "a+b%26""#,
        expected: "a b&",
    },
    TemplateFunctionCase {
        name: "urlencode",
        expression: r#"urlencode "a b&""#,
        expected: "a+b%26",
    },
    TemplateFunctionCase {
        name: "bytes",
        expression: r#"bytes "1KB""#,
        expected: "1000",
    },
    TemplateFunctionCase {
        name: "duration",
        expression: r#"duration "1m30s""#,
        expected: "90",
    },
    TemplateFunctionCase {
        name: "duration_seconds",
        expression: r#"duration_seconds "1m30s""#,
        expected: "90",
    },
    TemplateFunctionCase {
        name: "unixEpochMillis",
        expression: r#"unixEpochMillis (toDate "2006-01-02" "2024-01-02")"#,
        expected: "1704153600000",
    },
    TemplateFunctionCase {
        name: "unixEpochNanos",
        expression: r#"unixEpochNanos (toDate "2006-01-02" "2024-01-02")"#,
        expected: "1704153600000000000",
    },
    TemplateFunctionCase {
        name: "toDateInZone",
        expression: r#"toDateInZone "2006-01-02 15:04" "America/New_York" "2024-01-01 12:00" | unixEpoch"#,
        expected: "1704128400",
    },
    TemplateFunctionCase {
        name: "unixToTime",
        expression: r#"unixToTime "1704153600" | unixEpoch"#,
        expected: "1704153600",
    },
    TemplateFunctionCase {
        name: "alignLeft",
        expression: r#"alignLeft 5 "hi""#,
        expected: "hi   ",
    },
    TemplateFunctionCase {
        name: "alignRight",
        expression: r#"alignRight 5 "hi""#,
        expected: "   hi",
    },
    TemplateFunctionCase {
        name: "and",
        expression: r#"and "left" "right""#,
        expected: "right",
    },
    TemplateFunctionCase {
        name: "or",
        expression: r#"or "" "chosen""#,
        expected: "chosen",
    },
    TemplateFunctionCase {
        name: "not",
        expression: "not false",
        expected: "true",
    },
    TemplateFunctionCase {
        name: "eq",
        expression: r#"eq "b" "a" "b""#,
        expected: "true",
    },
    TemplateFunctionCase {
        name: "ne",
        expression: "ne 1 2",
        expected: "true",
    },
    TemplateFunctionCase {
        name: "lt",
        expression: "lt 1 2",
        expected: "true",
    },
    TemplateFunctionCase {
        name: "le",
        expression: "le 2 2",
        expected: "true",
    },
    TemplateFunctionCase {
        name: "gt",
        expression: "gt 2 1",
        expected: "true",
    },
    TemplateFunctionCase {
        name: "ge",
        expression: "ge 2 2",
        expected: "true",
    },
    TemplateFunctionCase {
        name: "len",
        expression: r#"len (fromJson "[1,2]")"#,
        expected: "2",
    },
    TemplateFunctionCase {
        name: "index",
        expression: r#"index (fromJson "[\"a\",\"b\"]") 1"#,
        expected: "b",
    },
    TemplateFunctionCase {
        name: "slice",
        expression: r#"slice "abcdef" 1 4"#,
        expected: "bcd",
    },
    TemplateFunctionCase {
        name: "print",
        expression: r#"print "a" 1"#,
        expected: "a1",
    },
    TemplateFunctionCase {
        name: "println",
        expression: "println 1 2",
        expected: "1 2\n",
    },
    TemplateFunctionCase {
        name: "printf",
        expression: r#"printf "%s-%v" "a" 1"#,
        expected: "a-1",
    },
    TemplateFunctionCase {
        name: "html",
        expression: r#"html "<a>&""#,
        expected: "&lt;a&gt;&amp;",
    },
    TemplateFunctionCase {
        name: "js",
        expression: r#"js "<a>""#,
        expected: "\\u003Ca\\u003E",
    },
    TemplateFunctionCase {
        name: "urlquery",
        expression: r#"urlquery "a b&""#,
        expected: "a+b%26",
    },
    TemplateFunctionCase {
        name: "line",
        expression: "__line__",
        expected: "raw",
    },
    TemplateFunctionCase {
        name: "int_base_and_fraction",
        expression: r#"int "0x10" | add (int "3.9") (int "010") (int "0b11")"#,
        expected: "30",
    },
    TemplateFunctionCase {
        name: "int_underscore",
        expression: r#"int "0x_FF" | add (int "1_000")"#,
        expected: "1255",
    },
    TemplateFunctionCase {
        name: "int_boolean",
        expression: r#"add (int true) (int false) (int "true")"#,
        expected: "1",
    },
    TemplateFunctionCase {
        name: "int_typed_fraction",
        expression: "add (int 3.9) (int -3.9)",
        expected: "0",
    },
    TemplateFunctionCase {
        name: "int_invalid_zero",
        expression: r#"add "invalid" 2"#,
        expected: "2",
    },
    TemplateFunctionCase {
        name: "integer_wrap",
        expression: "add 9223372036854775807 1",
        expected: "-9223372036854775808",
    },
    TemplateFunctionCase {
        name: "integer_comparison_precision",
        expression: "eq 9007199254740992 9007199254740993",
        expected: "false",
    },
    TemplateFunctionCase {
        name: "float_invalid_zero",
        expression: r#"float64 "invalid""#,
        expected: "0",
    },
    TemplateFunctionCase {
        name: "float_exponent",
        expression: "float64 1000000",
        expected: "1e+06",
    },
    TemplateFunctionCase {
        name: "float_small_exponent",
        expression: "float64 0.00001",
        expected: "1e-05",
    },
    TemplateFunctionCase {
        name: "float_negative_zero",
        expression: r#"float64 "-0""#,
        expected: "-0",
    },
    TemplateFunctionCase {
        name: "float_nan",
        expression: r#"float64 "NaN""#,
        expected: "NaN",
    },
    TemplateFunctionCase {
        name: "float_infinity",
        expression: r#"float64 "+Inf""#,
        expected: "+Inf",
    },
    TemplateFunctionCase {
        name: "float_nan_comparison",
        expression: r#"ne (float64 "NaN") (float64 "NaN")"#,
        expected: "true",
    },
    TemplateFunctionCase {
        name: "round_negative",
        expression: "round -1.2 0",
        expected: "-2",
    },
    TemplateFunctionCase {
        name: "round_cutoff",
        expression: "round 1.26 1 0.9",
        expected: "1.2",
    },
    TemplateFunctionCase {
        name: "json_float_precision",
        expression: r#"index (fromJson "[9007199254740993]") 0"#,
        expected: "9.007199254740992e+15",
    },
    TemplateFunctionCase {
        name: "json_float_comparison",
        expression: r#"eq (index (fromJson "[2]") 0) 2.0"#,
        expected: "true",
    },
    TemplateFunctionCase {
        name: "typed_length",
        expression: r#"eq (len (fromJson "[1,2]")) 2"#,
        expected: "true",
    },
    TemplateFunctionCase {
        name: "index_missing_map",
        expression: r#"index (fromJson "{}") "missing""#,
        expected: "<no value>",
    },
    TemplateFunctionCase {
        name: "index_string_byte",
        expression: r#"index "abc" 1"#,
        expected: "98",
    },
    TemplateFunctionCase {
        name: "decimal_addition",
        expression: "addf 0.1 0.2",
        expected: "0.3",
    },
    TemplateFunctionCase {
        name: "decimal_subtraction",
        expression: "subf 0.3 0.2",
        expected: "0.1",
    },
    TemplateFunctionCase {
        name: "decimal_multiplication",
        expression: "mulf 0.1 0.2",
        expected: "0.02",
    },
    TemplateFunctionCase {
        name: "decimal_division",
        expression: "divf 1 3",
        expected: "0.3333333333333333",
    },
    TemplateFunctionCase {
        name: "decimal_invalid_zero",
        expression: r#"addf "invalid" 2.5"#,
        expected: "2.5",
    },
    TemplateFunctionCase {
        name: "minf_nan",
        expression: r#"minf "NaN" 1"#,
        expected: "NaN",
    },
    TemplateFunctionCase {
        name: "maxf_infinity",
        expression: r#"maxf "+Inf" 1"#,
        expected: "+Inf",
    },
    TemplateFunctionCase {
        name: "minf_invalid_zero",
        expression: r#"minf "invalid" 1"#,
        expected: "0",
    },
    TemplateFunctionCase {
        name: "ceil_invalid_zero",
        expression: r#"ceil "invalid""#,
        expected: "0",
    },
    TemplateFunctionCase {
        name: "round_nonfinite",
        expression: "round 1.2 400",
        expected: "NaN",
    },
    TemplateFunctionCase {
        name: "nil_equality",
        expression: r#"eq (index (fromJson "{}") "missing") nil"#,
        expected: "true",
    },
    TemplateFunctionCase {
        name: "nil_inequality",
        expression: r#"ne "value" nil"#,
        expected: "true",
    },
    TemplateFunctionCase {
        name: "nil_default",
        expression: r#"default "fallback" nil"#,
        expected: "fallback",
    },
    TemplateFunctionCase {
        name: "integer_literal_bases",
        expression: "add 0xff 0b11 0o10 010 1_000",
        expected: "1274",
    },
    TemplateFunctionCase {
        name: "integer_literal_typed_conversion",
        expression: r#"indent 2.0 "x""#,
        expected: "  x",
    },
    TemplateFunctionCase {
        name: "float_literal_typed_conversion",
        expression: "round 1.29 1 1",
        expected: "1.2",
    },
    TemplateFunctionCase {
        name: "bytes_fractional_truncation",
        expression: r#"bytes "1.5""#,
        expected: "1",
    },
    TemplateFunctionCase {
        name: "bytes_lowercase_units",
        expression: r#"bytes "1,024 mib""#,
        expected: "1.073741824e+09",
    },
    TemplateFunctionCase {
        name: "duration_signed_fraction",
        expression: r#"duration "-.5h1m""#,
        expected: "-1860",
    },
    TemplateFunctionCase {
        name: "duration_unordered_units",
        expression: r#"duration "1s2m1s""#,
        expected: "122",
    },
    TemplateFunctionCase {
        name: "duration_micro_units",
        expression: r#"duration "1µs2μs""#,
        expected: "3e-06",
    },
    TemplateFunctionCase {
        name: "duration_subnanosecond",
        expression: r#"duration "0.9ns""#,
        expected: "0",
    },
    TemplateFunctionCase {
        name: "duration_minimum",
        expression: r#"duration "-9223372036854775808ns""#,
        expected: "-9.223372036854776e+09",
    },
    TemplateFunctionCase {
        name: "bytes_typed_result",
        expression: r#"printf "%T" (bytes "1KB")"#,
        expected: "float64",
    },
    TemplateFunctionCase {
        name: "rune_literal",
        expression: "printf \"%d %c\" 'λ' '\\u03bb'",
        expected: "955 λ",
    },
    TemplateFunctionCase {
        name: "quoted_hex_unicode",
        expression: r#"printf "%s" "\x41\101\u03bb\U0001f642""#,
        expected: "AAλ🙂",
    },
    TemplateFunctionCase {
        name: "byte_literal_identity",
        expression: r#"b64enc "\xff\xfe""#,
        expected: "//4=",
    },
    TemplateFunctionCase {
        name: "unicode_byte_substring",
        expression: r#"b64enc (substr 0 1 "é")"#,
        expected: "ww==",
    },
    TemplateFunctionCase {
        name: "unicode_byte_truncation",
        expression: r#"b64enc (trunc -1 "é")"#,
        expected: "qQ==",
    },
    TemplateFunctionCase {
        name: "unicode_simple_upper",
        expression: r#"upper "straße""#,
        expected: "STRAßE",
    },
    TemplateFunctionCase {
        name: "unicode_simple_lower",
        expression: r#"lower "İ""#,
        expected: "i",
    },
    TemplateFunctionCase {
        name: "title_preserves_case",
        expression: r#"title "hELLO_wORLD-go""#,
        expected: "HELLO_wORLD-Go",
    },
    TemplateFunctionCase {
        name: "raw_replace_capture",
        expression: r#"b64enc (regexReplaceAll "(.)(.)" "\xff\xfe" "$2$1")"#,
        expected: "/v8=",
    },
    TemplateFunctionCase {
        name: "raw_trim_identity",
        expression: r#"b64enc (trim " \xff\xfe " )"#,
        expected: "//4=",
    },
    TemplateFunctionCase {
        name: "base64_decode_crlf",
        expression: r#"b64dec "Y\nWJj\r\n""#,
        expected: "abc",
    },
    TemplateFunctionCase {
        name: "base64_decode_invalid",
        expression: r#"b64dec "not-base64!""#,
        expected: "illegal base64 data at input byte 3",
    },
    TemplateFunctionCase {
        name: "raw_url_roundtrip",
        expression: r#"b64enc (urldecode (urlencode "\xff\x00 "))"#,
        expected: "/wAg",
    },
    TemplateFunctionCase {
        name: "alignment_negative",
        expression: r#"alignLeft -1 "abc""#,
        expected: "abc",
    },
    TemplateFunctionCase {
        name: "incomplete_utf8_boundary",
        expression: r#"print "\xe2\x82""#,
        expected: "��",
    },
    TemplateFunctionCase {
        name: "literal_leading_fraction",
        expression: "printf \"%T:%v\" .5 .5",
        expected: "float64:0.5",
    },
    TemplateFunctionCase {
        name: "literal_hex_float",
        expression: "printf \"%T:%v\" 0x_1.fp2 0x_1.fp2",
        expected: "float64:7.75",
    },
    TemplateFunctionCase {
        name: "literal_complex",
        expression: "printf \"%T:%v\" 1+2i 1+2i",
        expected: "complex128:(1+2i)",
    },
    TemplateFunctionCase {
        name: "cast_hex_float",
        expression: "float64 \"0x1.fp2\"",
        expected: "7.75",
    },
    TemplateFunctionCase {
        name: "rune_byte_escape",
        expression: "printf \"%d:%d:%d\" '\\xff' '\\377' '\\xe2'",
        expected: "255:255:226",
    },
    TemplateFunctionCase {
        name: "time_method_year",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Year",
        expected: "2024",
    },
    TemplateFunctionCase {
        name: "time_method_month",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Month",
        expected: "February",
    },
    TemplateFunctionCase {
        name: "time_method_day",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Day",
        expected: "29",
    },
    TemplateFunctionCase {
        name: "time_method_weekday",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Weekday",
        expected: "Thursday",
    },
    TemplateFunctionCase {
        name: "time_method_year_day",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").YearDay",
        expected: "60",
    },
    TemplateFunctionCase {
        name: "time_method_hour",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Hour",
        expected: "13",
    },
    TemplateFunctionCase {
        name: "time_method_minute",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Minute",
        expected: "4",
    },
    TemplateFunctionCase {
        name: "time_method_second",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Second",
        expected: "5",
    },
    TemplateFunctionCase {
        name: "time_method_nanosecond",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Nanosecond",
        expected: "123456789",
    },
    TemplateFunctionCase {
        name: "time_method_unix",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Unix",
        expected: "1709211845",
    },
    TemplateFunctionCase {
        name: "time_method_unix_milli",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").UnixMilli",
        expected: "1709211845123",
    },
    TemplateFunctionCase {
        name: "time_method_unix_micro",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").UnixMicro",
        expected: "1709211845123456",
    },
    TemplateFunctionCase {
        name: "time_method_unix_nano",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").UnixNano",
        expected: "1709211845123456789",
    },
    TemplateFunctionCase {
        name: "time_method_zero_false",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").IsZero",
        expected: "false",
    },
    TemplateFunctionCase {
        name: "time_method_dst_false",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").IsDST",
        expected: "false",
    },
    TemplateFunctionCase {
        name: "time_method_utc",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").UTC",
        expected: "2024-02-29 13:04:05.123456789 +0000 UTC",
    },
    TemplateFunctionCase {
        name: "time_method_location",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Location",
        expected: "UTC",
    },
    TemplateFunctionCase {
        name: "time_method_format",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Format \"2006/01/02\"",
        expected: "2024/02/29",
    },
    TemplateFunctionCase {
        name: "time_method_format_bytes",
        expression: "printf \"%x\" ((toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Format \"2006\\xff01\")",
        expected: "32303234ff3032",
    },
    TemplateFunctionCase {
        name: "time_method_add",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Add 1500000000",
        expected: "2024-02-29 13:04:06.623456789 +0000 UTC",
    },
    TemplateFunctionCase {
        name: "time_method_add_date",
        expression: "((toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").AddDate 1 1 -2).Format \"2006-01-02 15:04:05.999999999 MST\"",
        expected: "2025-03-27 13:04:05.123456789 UTC",
    },
    TemplateFunctionCase {
        name: "time_method_before",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Before ((toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Add 1)",
        expected: "true",
    },
    TemplateFunctionCase {
        name: "time_method_equal",
        expression: "(toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Equal (toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").UTC",
        expected: "true",
    },
    TemplateFunctionCase {
        name: "time_method_duration",
        expression: "printf \"%T:%v:%.6f\" ((toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Sub ((toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Add -1500000000)) ((toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Sub ((toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Add -1500000000)) ((toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Sub ((toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Add -1500000000)).Seconds",
        expected: "time.Duration:1.5s:1.500000",
    },
    TemplateFunctionCase {
        name: "time_method_duration_abs",
        expression: "((toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Sub ((toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Add -1500000000)).Abs.Nanoseconds",
        expected: "1500000000",
    },
    TemplateFunctionCase {
        name: "time_method_binary",
        expression: "printf \"%T:%x\" (toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").MarshalBinary (toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").MarshalBinary",
        expected: "[]uint8:010000000edd7277c5075bcd15ffff",
    },
    TemplateFunctionCase {
        name: "time_method_text",
        expression: "printf \"%T:%s\" (toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").MarshalText (toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").MarshalText",
        expected: "[]uint8:2024-02-29T13:04:05.123456789Z",
    },
    TemplateFunctionCase {
        name: "time_method_json",
        expression: "printf \"%s\" (toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").MarshalJSON",
        expected: "\"2024-02-29T13:04:05.123456789Z\"",
    },
    TemplateFunctionCase {
        name: "time_method_append_text",
        expression: "len ((toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").AppendText nil)",
        expected: "30",
    },
    TemplateFunctionCase {
        name: "time_method_byte_index",
        expression: "printf \"%T\" (index (toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").MarshalBinary 0)",
        expected: "uint8",
    },
    TemplateFunctionCase {
        name: "time_method_location_type",
        expression: "printf \"%T:%v\" (toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Location (toDate \"2006-01-02T15:04:05.999999999Z07:00\" \"2024-02-29T13:04:05.123456789Z\").Location",
        expected: "*time.Location:UTC",
    },
    TemplateFunctionCase {
        name: "time_method_dst_true",
        expression: "(toDateInZone \"2006-01-02\" \"America/New_York\" \"2024-07-01\").IsDST",
        expected: "true",
    },
];
