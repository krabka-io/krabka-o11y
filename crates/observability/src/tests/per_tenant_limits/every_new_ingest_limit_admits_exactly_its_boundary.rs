use super::*;

/// The four ingest-side caps share a shape: zero means no limit, a value
/// exactly at the limit is accepted, and one byte or one name over is
/// refused. Each is checked at all three points, because `>` and `>=`
/// differ only at the boundary.
///
/// The refusals are checked by variant rather than by `is_err`, because all
/// four produce a `DistributorError` and a mutant that swapped one check's
/// error for another's would otherwise survive.
#[test]
pub(crate) fn every_new_ingest_limit_admits_exactly_its_boundary() {
    let stream = |names: usize, value: &str| {
        let mut labels = Labels::default();
        for index in 0..names {
            labels.insert(format!("label_{index}"), value.to_string());
        }
        labels
    };
    let capped = |limits: Limits| limits;

    // Line size, measured in UTF-8 bytes of the line.
    let labels = stream(1, "v");
    let line = "0123456789";
    let line_limit = |max_line_size: ByteSize| {
        capped(Limits {
            max_line_size,
            ..Limits::unenforced()
        })
    };
    check!(
        validate_loki_line_size(line, &labels, &Limits::unenforced()).is_ok(),
        "off"
    );
    check!(
        validate_loki_line_size(line, &labels, &line_limit(bytes(10))).is_ok(),
        "exactly at the limit"
    );
    check!(matches!(
        validate_loki_line_size(line, &labels, &line_limit(bytes(9))),
        Err(DistributorError::LineTooLong { .. })
    ));

    // Label count.
    let count_limit = |max_label_names_per_series: u64| {
        capped(Limits {
            max_label_names_per_series,
            ..Limits::unenforced()
        })
    };
    check!(
        validate_loki_label_limits(&stream(3, "v"), &Limits::unenforced()).is_ok(),
        "off"
    );
    check!(
        validate_loki_label_limits(&stream(3, "v"), &count_limit(3)).is_ok(),
        "exactly at the limit"
    );
    check!(matches!(
        validate_loki_label_limits(&stream(3, "v"), &count_limit(2)),
        Err(DistributorError::TooManyLabelNames { .. })
    ));

    // Label name length. "label_0" is seven bytes.
    let name_limit = |max_label_name_length: ByteSize| {
        capped(Limits {
            max_label_name_length,
            ..Limits::unenforced()
        })
    };
    check!(
        validate_loki_label_limits(&stream(1, "v"), &name_limit(bytes(7))).is_ok(),
        "exactly at the limit"
    );
    check!(matches!(
        validate_loki_label_limits(&stream(1, "v"), &name_limit(bytes(6))),
        Err(DistributorError::LabelNameTooLong { .. })
    ));

    // Label value length.
    let value_limit = |max_label_value_length: ByteSize| {
        capped(Limits {
            max_label_value_length,
            ..Limits::unenforced()
        })
    };
    check!(
        validate_loki_label_limits(&stream(1, "abcd"), &value_limit(bytes(4))).is_ok(),
        "exactly at the limit"
    );
    check!(matches!(
        validate_loki_label_limits(&stream(1, "abcd"), &value_limit(bytes(3))),
        Err(DistributorError::LabelValueTooLong { .. })
    ));

    // The ingest body cap, which is Krabka's own.
    let body_limit = Limits {
        max_ingest_body: bytes(100),
        ..Limits::unenforced()
    };
    check!(validate_ingest_body_limit(&body_limit, bytes(100)).is_ok());
    check!(matches!(
        validate_ingest_body_limit(&body_limit, bytes(101)),
        Err(DistributorError::IngestBodyTooLarge { .. })
    ));

    // Each refusal names the stream and the numbers a client needs.
    let error = validate_loki_line_size(line, &labels, &line_limit(bytes(9)))
        .expect_err("one byte over the line cap");
    let message = error.to_string();
    check!(message.contains("Max entry size '9'"), "{message}");
    check!(message.contains("length '10' bytes"), "{message}");
}
