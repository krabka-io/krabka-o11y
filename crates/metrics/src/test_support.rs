//! Fixtures the crate's unit tests share.

use bytes::Bytes;
use krabka_client_consumer::{ConsumerRecord, Header, TimestampType};
use krabka_observability::persisted_format::{PERSISTED_FORMAT_HEADER, PERSISTED_FORMAT_VERSION};

// The headers every record Krabka produces carries.
pub fn format_headers() -> Vec<Header> {
    vec![Header {
        key: PERSISTED_FORMAT_HEADER.to_string(),
        value: Some(Bytes::from_static(PERSISTED_FORMAT_VERSION)),
    }]
}

// A record as a consumer delivers it, carrying the persisted-format header.
pub struct DeliveredRecord<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub offset: i64,
    // Its create time.
    pub timestamp: i64,
    pub payload: Option<Vec<u8>>,
}

impl Default for DeliveredRecord<'_> {
    fn default() -> Self {
        Self {
            topic: crate::WAL_TOPIC,
            partition: 0,
            offset: 0,
            timestamp: 0,
            payload: None,
        }
    }
}

impl DeliveredRecord<'_> {
    pub fn build(self) -> ConsumerRecord {
        ConsumerRecord {
            topic: self.topic.to_string(),
            partition: self.partition,
            offset: self.offset,
            leader_epoch: -1,
            timestamp: self.timestamp,
            timestamp_type: TimestampType::CreateTime,
            key: None,
            value: self.payload.map(Bytes::from),
            headers: format_headers(),
        }
    }
}
