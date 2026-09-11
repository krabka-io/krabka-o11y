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

mod admin_operation;
mod audit_args;
mod audit_build_error;
mod audit_clocks;
mod audit_handle;
mod audit_principal_of;
mod audit_producer_record;
mod audit_security_events;
mod audit_service;
mod authentication;
mod authorization_denied;
mod kafka_topic_audit_sink;
mod krabka_product;
mod mechanism_of;
mod mechanisms;
mod operations;
mod principal;
mod resource;
mod resource_types;
mod source_endpoint;
#[cfg(test)]
mod tests;
mod unauthenticated_principal;
mod unknown_source_endpoint;

pub use krabka_audit::{
    AuditEndpoint, AuditError, AuditEvent, AuditOutcome, AuditPrincipal, AuditRecord,
    AuditResource, AuditSink, EpochMs, ProductInfo,
};

use self::audit_producer_record::audit_producer_record;
pub use self::{
    admin_operation::admin_operation,
    audit_args::{
        AuditArgs, DEFAULT_AUDIT_CHECKPOINT_EVERY, DEFAULT_AUDIT_QUEUE_CAPACITY,
        DEFAULT_AUDIT_SPOOL_MAX,
    },
    audit_build_error::AuditBuildError,
    audit_clocks::AuditClocks,
    audit_handle::AuditHandle,
    audit_principal_of::audit_principal_of,
    audit_service::{AUDIT_CHECKPOINT_EVERY_RECORDS, AUDIT_SPOOL_REPLAY_EVERY, AuditService},
    authentication::authentication,
    authorization_denied::authorization_denied,
    kafka_topic_audit_sink::{DEFAULT_AUDIT_WRITE_TIMEOUT, KafkaTopicAuditSink},
    krabka_product::krabka_product,
    mechanism_of::mechanism_of,
    mechanisms::{MECHANISM_BASIC, MECHANISM_BEARER, MECHANISM_MTLS, MECHANISM_NONE, MECHANISMS},
    operations::{
        OPERATION_ADMIN_ACCESS, OPERATION_DELETE_REQUEST_CANCEL, OPERATION_DELETE_REQUEST_CREATE,
        OPERATION_INGESTER_FLUSH, OPERATION_INGESTER_PREPARE_SHUTDOWN_SET,
        OPERATION_INGESTER_PREPARE_SHUTDOWN_UNSET, OPERATION_INGESTER_SHUTDOWN,
        OPERATION_LOG_LEVEL_SET, OPERATION_RULE_GROUP_DELETE, OPERATION_RULE_GROUP_SET,
        OPERATION_RULE_NAMESPACE_DELETE, OPERATION_TENANT_ACCESS, OPERATION_TENANT_READ,
        OPERATION_TENANT_WRITE, OPERATIONS,
    },
    principal::principal,
    resource::resource,
    resource_types::{
        RESOURCE_ADMIN_API, RESOURCE_DELETE_REQUEST, RESOURCE_INGESTER, RESOURCE_LOG_LEVEL,
        RESOURCE_RULE_GROUP, RESOURCE_RULE_NAMESPACE, RESOURCE_TENANT, RESOURCE_TYPES,
        RESOURCE_WAL_TOPIC,
    },
    source_endpoint::source_endpoint,
    unauthenticated_principal::{UNAUTHENTICATED_PRINCIPAL_NAME, unauthenticated_principal},
    unknown_source_endpoint::unknown_source_endpoint,
};
