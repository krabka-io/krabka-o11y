//! The audit trail for tenant-affecting and destructive operations.
//!
//! A service records who did an operation, from which address, and with which
//! result. The operations are a deleted log range, a changed rule group, a
//! changed log level, an ingester flush or shutdown, and a refused tenant.
//! [`krabka_audit`] turns each event into an OCSF record, adds it to a hash
//! chain, and writes it to a Kafka topic. The Krabka broker writes its own
//! audit trail with the same crate, so one verifier reads both trails.
//!
//! # Opt-in
//!
//! Audit is off until an operator sets `--audit-topic`. With no audit flags,
//! [`AuditService::start`] gives a disabled handle and spawns no task, and the
//! service behaves as it does with no audit layer.
//!
//! # Call sites
//!
//! A request handler holds an [`AuditHandle`] and calls
//! [`AuditHandle::admin_operation`], [`AuditHandle::authorization_denied`] or
//! [`AuditHandle::authentication`]. These calls do not block and do not fail.
//! When the queue is full, the handle drops the event and counts it in
//! [`AuditHandle::dropped`]. A service should export that count as a metric.
//!
//! The operation, resource-type and mechanism strings are closed sets:
//! [`OPERATIONS`], [`RESOURCE_TYPES`] and [`MECHANISMS`]. A query or a
//! dashboard can match on these strings.
//!
//! # Write path
//!
//! [`AuditService`] spawns one `krabka_audit::AuditWriter` for each process.
//! The writer adds each event to the chain and gives the record to a
//! [`KafkaTopicAuditSink`], which writes every record to one partition. When a
//! write fails, the writer puts records in the spool at `--audit-spool-dir`.
//! It writes them again, in order, when the topic accepts writes again.

mod audit_args;
mod audit_build_error;
mod audit_clocks;
mod audit_handle;
mod audit_security_events;
mod audit_service;
mod kafka_topic_audit_sink;
mod mechanisms;
mod operations;
mod resource_types;
#[cfg(test)]
mod tests;

pub use krabka_audit::{
    AuditEndpoint, AuditError, AuditEvent, AuditOutcome, AuditPrincipal, AuditRecord,
    AuditResource, AuditSink, EpochMs, ProductInfo,
};

use axum::body::Bytes;
use krabka_client_producer::{Header, ProducerRecord};
use krabka_ids::PartitionIndex;
use crate::server_security::Principal;

pub use self::{
    audit_args::{
        AuditArgs, DEFAULT_AUDIT_CHECKPOINT_EVERY, DEFAULT_AUDIT_QUEUE_CAPACITY,
        DEFAULT_AUDIT_SPOOL_MAX,
    },
    audit_build_error::AuditBuildError,
    audit_clocks::AuditClocks,
    audit_handle::AuditHandle,
    audit_service::{
        AUDIT_CHECKPOINT_EVERY_RECORDS, AUDIT_SPOOL_REPLAY_EVERY, AuditService, AuditServiceParts,
    },
    kafka_topic_audit_sink::{DEFAULT_AUDIT_WRITE_TIMEOUT, KafkaTopicAuditSink},
    mechanisms::{mechanism_of, MECHANISM_BASIC, MECHANISM_BEARER, MECHANISM_MTLS, MECHANISM_NONE, MECHANISMS},
    operations::{
        OPERATION_ADMIN_ACCESS, OPERATION_DELETE_REQUEST_CANCEL, OPERATION_DELETE_REQUEST_CREATE,
        OPERATION_INGESTER_FLUSH, OPERATION_INGESTER_PREPARE_SHUTDOWN_SET,
        OPERATION_INGESTER_PREPARE_SHUTDOWN_UNSET, OPERATION_INGESTER_SHUTDOWN,
        OPERATION_LOG_LEVEL_SET, OPERATION_RULE_GROUP_DELETE, OPERATION_RULE_GROUP_SET,
        OPERATION_RULE_NAMESPACE_DELETE, OPERATION_TENANT_ACCESS, OPERATION_TENANT_READ,
        OPERATION_TENANT_WRITE, OPERATIONS,
    },
    resource_types::{
        RESOURCE_ADMIN_API, RESOURCE_DELETE_REQUEST, RESOURCE_INGESTER, RESOURCE_LOG_LEVEL,
        RESOURCE_RULE_GROUP, RESOURCE_RULE_NAMESPACE, RESOURCE_TENANT, RESOURCE_TYPES,
        RESOURCE_WAL_TOPIC,
    },
};

/// An event for an operation that changes a tenant's data or the service
/// itself, at `time`.
///
/// `operation` is one of [`OPERATIONS`](OPERATIONS). Every resource
/// type in `resources` is one of [`RESOURCE_TYPES`](RESOURCE_TYPES).
/// Use [`AuditOutcome::Failure`] when the service tried the operation and the
/// operation failed. Record an authorization refusal with
/// [`authorization_denied`](authorization_denied).
#[must_use]
pub fn admin_operation(
    principal: AuditPrincipal,
    source: AuditEndpoint,
    operation: &'static str,
    resources: Vec<AuditResource>,
    outcome: AuditOutcome,
    time: EpochMs,
) -> AuditEvent {
    AuditEvent::AdminOperation {
        outcome,
        principal,
        source,
        operation: operation.to_owned(),
        resources,
        time_ms: time.into(),
    }
}

/// The audit principal for the principal the authentication layer put on a request.
///
/// An unauthenticated request becomes [`unauthenticated_principal`], so an
/// audit record for a server without a credentials file names no principal
/// that a credentials file could define.
#[must_use]
pub fn audit_principal_of(request_principal: &Principal) -> AuditPrincipal {
    match request_principal {
        Principal::Unauthenticated => unauthenticated_principal(),
        Principal::Authenticated { name, method, .. } => {
            principal(name.as_ref(), mechanism_of(*method))
        }
    }
}

/// The Kafka record for one audit record, pinned to `partition`.
///
/// The record carries every audit header, and the chain headers with them. It
/// has no key, because the partition is fixed. The producer stamps the record
/// time. The event time is in the OCSF value.
pub fn audit_producer_record(
    topic: &str,
    partition: PartitionIndex,
    record: AuditRecord,
) -> ProducerRecord {
    ProducerRecord {
        topic: topic.to_owned(),
        partition: Some(partition.get()),
        key: None,
        value: Some(Bytes::from(record.value)),
        headers: std::iter::once(Header {
            key: crate::persisted_format::PERSISTED_FORMAT_HEADER.to_string(),
            value: Some(Bytes::from_static(
                crate::persisted_format::PERSISTED_FORMAT_VERSION,
            )),
        })
        .chain(record.headers.into_iter().map(|header| Header {
            key: header.0,
            value: Some(Bytes::from(header.1)),
        }))
        .collect(),
        timestamp_ms: None,
    }
}

/// An event for an authentication attempt with `mechanism`, at `time`.
///
/// `mechanism` is one of [`MECHANISMS`](MECHANISMS). `reason` tells
/// why a failed attempt failed. It should not contain the credentials.
#[must_use]
pub fn authentication(
    outcome: AuditOutcome,
    mechanism: &'static str,
    principal: AuditPrincipal,
    source: AuditEndpoint,
    reason: Option<String>,
    time: EpochMs,
) -> AuditEvent {
    AuditEvent::Authentication {
        outcome,
        mechanism: mechanism.to_owned(),
        principal,
        source,
        reason,
        time_ms: time.into(),
    }
}

/// An event for a request that the service refused because the principal may
/// not do `operation` on the resource, at `time`.
///
/// Use it for a WAL topic ACL refusal and for a principal that may not use
/// the tenant it names. `resource_type` is one of
/// [`RESOURCE_TYPES`](RESOURCE_TYPES), and `operation` is one of
/// [`OPERATIONS`](OPERATIONS).
#[must_use]
pub fn authorization_denied(
    principal: AuditPrincipal,
    source: AuditEndpoint,
    resource_type: &'static str,
    resource_name: impl Into<String>,
    operation: &'static str,
    time: EpochMs,
) -> AuditEvent {
    AuditEvent::AuthorizationDenied {
        principal,
        source,
        resource_type: resource_type.to_owned(),
        resource_name: resource_name.into(),
        operation: operation.to_owned(),
        time_ms: time.into(),
    }
}

/// The vendor name in every Krabka audit record. The broker uses the same name.
const VENDOR_NAME: &str = "Krabka";

/// The product identity that each audit record carries, for the service
/// `name` at `version`.
///
/// A service binary should pass its package name and
/// `env!("CARGO_PKG_VERSION")`.
#[must_use]
pub fn krabka_product(name: &str, version: &str) -> ProductInfo {
    ProductInfo {
        vendor_name: VENDOR_NAME.to_owned(),
        name: name.to_owned(),
        version: version.to_owned(),
    }
}

/// The principal `name`, authenticated with `mechanism`.
///
/// `mechanism` is one of [`MECHANISMS`](MECHANISMS).
#[must_use]
pub fn principal(name: impl Into<String>, mechanism: &'static str) -> AuditPrincipal {
    AuditPrincipal {
        name: name.into(),
        auth_method: mechanism.to_owned(),
    }
}

/// The resource `name` of the type `resource_type`.
///
/// `resource_type` is one of [`RESOURCE_TYPES`](RESOURCE_TYPES).
#[must_use]
pub fn resource(resource_type: &'static str, name: impl Into<String>) -> AuditResource {
    AuditResource {
        resource_type: resource_type.to_owned(),
        name: name.into(),
    }
}

/// The network source of a request that arrived from `address`.
///
/// An IPv4 address in IPv6 form, such as `::ffff:10.0.0.1`, becomes its IPv4
/// form, so one client has one address in the trail.
#[must_use]
pub fn source_endpoint(address: std::net::SocketAddr) -> AuditEndpoint {
    AuditEndpoint {
        ip: address.ip().to_canonical().to_string(),
        port: address.port(),
    }
}

/// The principal name for a request that carries no credentials.
///
/// The angle brackets keep this name apart from a user name. A credentials
/// file should not define a user with this name.
pub const UNAUTHENTICATED_PRINCIPAL_NAME: &str = "<unauthenticated>";

/// The principal of a request that carries no credentials.
///
/// An event for such a request should also name the tenant from the request
/// as a resource, because the principal does not identify the caller.
#[must_use]
pub fn unauthenticated_principal() -> AuditPrincipal {
    AuditPrincipal {
        name: UNAUTHENTICATED_PRINCIPAL_NAME.to_owned(),
        auth_method: MECHANISM_NONE.to_owned(),
    }
}

/// The network source for a request whose peer address the service does not
/// know: `0.0.0.0`, port `0`.
#[must_use]
pub fn unknown_source_endpoint() -> AuditEndpoint {
    AuditEndpoint {
        ip: std::net::Ipv4Addr::UNSPECIFIED.to_string(),
        port: 0,
    }
}
