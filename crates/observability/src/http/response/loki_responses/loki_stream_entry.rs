use serde::{Serialize, Serializer, ser::SerializeSeq as _};

use super::{Labels, Value, json};

/// One entry of a Loki `streams` result, with its labels kept apart by origin.
///
/// Loki's default JSON encoding serialises an entry as the two-element array
/// `["<ns>", "<line>"]` and folds everything the entry carried -- its
/// structured metadata, and any label a parser or `label_format` stage
/// produced -- into the stream's label map. One pushed stream therefore comes
/// back as one stream per distinct metadata value.
///
/// `X-Loki-Response-Encoding-Flags: categorize-labels` asks for the other
/// encoding: the stream keeps only its own labels, and the entry grows a third
/// element that sorts the rest into buckets,
/// `{"structuredMetadata": {...}, "parsed": {...}}`. An empty bucket is
/// omitted, and an entry that has nothing to categorise still gets `{}`.
///
/// The two encodings are the same data grouped differently, so an entry holds
/// both buckets whichever encoding the request asked for, and the response
/// builder folds or splits accordingly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LokiStreamEntry {
    pub(crate) timestamp_ns: String,
    pub(crate) line: String,
    pub(crate) source_labels: Labels,
    pub(crate) structured_metadata: Labels,
    pub(crate) parsed: Labels,
}

impl LokiStreamEntry {
    /// Builds an entry from a line and the two label buckets behind it.
    pub(crate) fn new(
        timestamp_ns: i64,
        line: String,
        structured_metadata: Labels,
        parsed: Labels,
    ) -> Self {
        Self {
            timestamp_ns: timestamp_ns.to_string(),
            line,
            source_labels: Labels::new(),
            structured_metadata,
            parsed,
        }
    }

    /// The entry's timestamp, or `None` when it is not a decimal integer.
    pub(crate) fn parsed_timestamp_ns(&self) -> Option<i64> {
        self.timestamp_ns.parse::<i64>().ok()
    }

    /// The entry under the `categorize-labels` encoding: three elements, the
    /// last one an envelope that names each bucket it has anything to put in.
    pub(crate) fn categorized_value(&self) -> Value {
        let mut envelope = json!({});
        if !self.structured_metadata.is_empty() {
            envelope["structuredMetadata"] = json!(self.structured_metadata);
        }
        if !self.parsed.is_empty() {
            envelope["parsed"] = json!(self.parsed);
        }
        json!([self.timestamp_ns, self.line, envelope])
    }

    /// The labels this entry carries in a bucket of its own, which the default
    /// encoding folds into the stream and `categorize-labels` keeps out of it.
    pub(crate) fn categorized_label_names(&self) -> impl Iterator<Item = &String> {
        self.structured_metadata.keys().chain(self.parsed.keys())
    }
}

impl Serialize for LokiStreamEntry {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut entry = serializer.serialize_seq(Some(2))?;
        entry.serialize_element(&self.timestamp_ns)?;
        entry.serialize_element(&self.line)?;
        entry.end()
    }
}
