use std::{sync::atomic::AtomicUsize, time::Duration};

use super::prelude::{
    AclEntry, AclEntryFilter, AclOperation, AclSet, AdminError, Arc, AtomicBool, AtomicOrdering,
    AuthMethod, BTreeMap, BrokerAccessCache, BrokerAccessPolicy, BrokerAccessSource,
    BrokerBackedIngestLimiter, BrokerBackedQueryAuthorizer, CachedLookup, CachedLookupDecision,
    CancellationToken, IngestLimitError, Instant, LogIngestLimiter, LogQueryAuthorizer, Mutex,
    NonZeroUsize, PatternType, PermissionType, Principal, QueryAuthorizationError, ResourceType,
    SecurityEventSink, ServiceConfig, ServiceConfigError, TenantGrant, TenantId, TenantLru, Time,
    TimeExt, WalLogRecord, acl_set_from_describe, async_trait, check, check_tenant_wal_read_acl,
    check_tenant_wal_write_acl, millis, secs, wal_topic_acl_filters,
};

mod a_cached_lookup_serves_refreshes_or_fails_closed_by_its_age;
mod a_failed_refresh_keeps_serving_the_last_snapshot_until_the_staleness_bound;
mod broker_access_for_test;
mod counting_broker_access;
mod keep_fresh_asks_the_broker_once_per_ttl_until_cancelled;
mod no_snapshot_and_an_unreachable_broker_fail_closed_without_a_queue;
mod only_security_disabled_turns_a_describe_error_into_an_allow;
mod repeated_checks_inside_the_ttl_ask_the_broker_once;
mod the_quota_cache_and_the_rate_buckets_stay_within_the_tenant_capacity;
mod the_staleness_bound_may_not_be_shorter_than_the_ttl;
mod the_tenant_lru_removes_the_least_recently_used_tenant;
mod the_wal_topic_acl_filters_cover_the_topic_the_wildcard_and_every_prefix;
mod the_wal_topic_acls_decide_by_their_state_and_the_principal_kind;

pub(crate) use broker_access_for_test::{broker_access_for_test, policy_for_test, tenant_for_test};
pub(crate) use counting_broker_access::CountingBrokerAccess;
