// What the suites that push spans through the distributor's doors share: an
// OTLP string attribute to put on the pushed spans, and a WAL sink that keeps
// what the doors append.

use std::sync::{Arc, Mutex};

use krabka_traces::{SpanRecord, TracesError, distributor::WalSink};
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, KeyValue as OtlpKeyValue, any_value::Value,
};

/// A WAL sink that keeps every record appended to it, in order, instead of
/// sending it to Kafka.
#[derive(Clone, Default)]
pub struct CapturingSink {
    pub records: Arc<Mutex<Vec<SpanRecord>>>,
}

#[async_trait::async_trait]
impl WalSink for CapturingSink {
    async fn append(&self, rec: SpanRecord) -> Result<(), TracesError> {
        self.records
            .lock()
            .map_err(|_| TracesError::Wal("capturing sink lock poisoned".into()))?
            .push(rec);
        Ok(())
    }
}

/// An OTLP attribute with a string value.
pub fn string_kv(key: &str, value: &str) -> OtlpKeyValue {
    OtlpKeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
        ..OtlpKeyValue::default()
    }
}
