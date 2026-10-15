use krabka_logql::hex_digit_value;
use krabka_units::convert::TimeExt;

use crate::{
    HeaderMap, HttpQueryError, QuerierState, QueryParams, RequestSecurity, SystemTime,
    TenantErrorSurface, TenantId, Time, TimeRange, UNIX_EPOCH, hours, resolve_federated_tenants,
    resolve_single_tenant, secs, tenant_header_value,
};

/// Window a query covers when it names neither `start` nor `since`.
pub(crate) const LOKI_DEFAULT_QUERY_RANGE: Time = hours(1);

pub(crate) const LOKI_DEFAULT_TAIL_LIMIT: usize = 100;

pub(crate) const LOKI_MAX_QUERY_RANGE_RESOLUTION_POINTS: i64 = 11_000;

/// Longest `delay_for` a tail request may ask the querier to hold back.
pub(crate) const LOKI_MAX_TAIL_DELAY: Time = secs(5);

/// Window the metadata endpoints index over when the request names no range.
pub(crate) const LOKI_METADATA_DEFAULT_INDEX_RANGE: Time = hours(6);

pub(crate) fn current_unix_time_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
        })
}

pub(crate) fn optional_start_end_range(
    start: Option<i64>,
    since: Option<i64>,
    end: Option<i64>,
) -> Result<TimeRange, HttpQueryError> {
    let start = start_or_since(start, since, end)?.unwrap_or(i64::MIN);
    TimeRange::new(start, end.unwrap_or(i64::MAX)).map_err(HttpQueryError::from)
}

/// The one tenant a read names, after the principal's grant and the query
/// authorizer allow it.
///
/// `surface` picks which of Loki's answers a tenant error gets. See
/// [`TenantErrorSurface`]. A tenant outside the principal's grant gets a 403
/// before the query authorizer runs. The audit trail records a refusal from
/// the broker ACLs.
pub(crate) async fn authorized_tenant(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    surface: TenantErrorSurface,
) -> Result<TenantId, HttpQueryError> {
    let tenant = resolve_single_tenant(tenant_header_value(headers))
        .map_err(|source| HttpQueryError::Tenant { source, surface })?;
    security.authorize_tenant(&tenant)?;
    state
        .query_authorizer
        .check(&security.principal, &tenant)
        .await
        .inspect_err(|error| security.record_read_refusal(error))?;
    Ok(tenant)
}

pub(crate) fn parse_usize_query_param(
    name: &'static str,
    value: &str,
) -> Result<usize, HttpQueryError> {
    if name == "limit" {
        let limit = value
            .parse::<i64>()
            .map_err(|_| HttpQueryError::InvalidLimit(value.to_string()))?;
        if limit <= 0 {
            return Err(HttpQueryError::LimitNotPositive);
        }
        return usize::try_from(limit).map_err(|_| HttpQueryError::InvalidLimit(value.to_string()));
    }

    value
        .parse()
        .map_err(|_| HttpQueryError::InvalidQueryParameter {
            name,
            value: value.to_string(),
        })
}

pub(crate) fn start_or_since(
    start: Option<i64>,
    since: Option<i64>,
    end: Option<i64>,
) -> Result<Option<i64>, HttpQueryError> {
    if start.is_some() {
        return Ok(start);
    }
    let Some(since) = since else {
        return Ok(None);
    };
    if since <= 0 {
        return Err(HttpQueryError::InvalidSinceQueryParameter {
            value: since.to_string(),
        });
    }
    let Some(end) = end else {
        return Ok(None);
    };
    end.checked_sub(since)
        .map(Some)
        .ok_or_else(|| HttpQueryError::InvalidSinceQueryParameter {
            value: since.to_string(),
        })
}

/// Every tenant a federated query names, after the principal's grant and the
/// query authorizer allow each one.
///
/// The principal's grant must include every tenant before the query
/// authorizer runs for any of them, so a query that names one tenant outside
/// the grant gets a 403 and reads nothing.
///
/// Krabka federates a query as Loki does with
/// `querier.multi_tenant_queries_enabled` on. The `loki_differential`
/// configuration leaves that switch off, and Loki then answers `a|b` with 500
/// `multiple org IDs present`. That divergence is deliberate: the multi-tenant
/// query is a feature the querier keeps.
pub(crate) async fn authorized_tenants(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    surface: TenantErrorSurface,
) -> Result<Vec<TenantId>, HttpQueryError> {
    let tenants = resolve_federated_tenants(tenant_header_value(headers))
        .map_err(|source| HttpQueryError::Tenant { source, surface })?;
    for tenant in &tenants {
        security.authorize_tenant(tenant)?;
    }
    for tenant in &tenants {
        state
            .query_authorizer
            .check(&security.principal, tenant)
            .await
            .inspect_err(|error| security.record_read_refusal(error))?;
    }
    Ok(tenants)
}

pub(crate) fn decode_form_component(component: &str) -> Result<String, HttpQueryError> {
    let mut bytes = Vec::with_capacity(component.len());
    let mut iter = component.as_bytes().iter().copied();
    while let Some(byte) = iter.next() {
        match byte {
            b'+' => bytes.push(b' '),
            b'%' => {
                let high = iter
                    .next()
                    .and_then(hex_digit_value)
                    .ok_or(HttpQueryError::InvalidPercentEncoding)?;
                let low = iter
                    .next()
                    .and_then(hex_digit_value)
                    .ok_or(HttpQueryError::InvalidPercentEncoding)?;
                bytes.push(high << 4 | low);
            }
            _ => bytes.push(byte),
        }
    }

    String::from_utf8(bytes).map_err(|_| HttpQueryError::InvalidPercentEncoding)
}

/// One `key=value` pair of a URL query string or form body, percent-decoded.
pub(crate) struct DecodedQueryPair {
    pub(crate) key: String,
    pub(crate) value: String,
}

/// Decodes one `&`-separated query pair; a pair without `=` has an empty value.
pub(crate) fn decode_query_pair(pair: &str) -> Result<DecodedQueryPair, HttpQueryError> {
    let separator = pair.find('=');
    let key = separator.map_or(pair, |index| &pair[..index]);
    let value = separator.map_or("", |index| &pair[index + 1..]);
    Ok(DecodedQueryPair {
        key: decode_form_component(key)?,
        value: decode_form_component(value)?,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LokiDirection {
    Forward,
    Backward,
}

pub(crate) fn loki_direction(direction: Option<&str>) -> Result<LokiDirection, HttpQueryError> {
    let direction = direction.unwrap_or("");
    if direction.is_empty() || direction.eq_ignore_ascii_case("backward") {
        Ok(LokiDirection::Backward)
    } else if direction.eq_ignore_ascii_case("forward") {
        Ok(LokiDirection::Forward)
    } else {
        Err(HttpQueryError::InvalidDirection(direction.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn direction_defaults_and_case_match_lokis_http_parser() {
        struct DirectionCase {
            input: Option<&'static str>,
            expected: LokiDirection,
        }
        for case in [
            DirectionCase {
                input: None,
                expected: LokiDirection::Backward,
            },
            DirectionCase {
                input: Some(""),
                expected: LokiDirection::Backward,
            },
            DirectionCase {
                input: Some("backward"),
                expected: LokiDirection::Backward,
            },
            DirectionCase {
                input: Some("BACKWARD"),
                expected: LokiDirection::Backward,
            },
            DirectionCase {
                input: Some("BaCkWaRd"),
                expected: LokiDirection::Backward,
            },
            DirectionCase {
                input: Some("forward"),
                expected: LokiDirection::Forward,
            },
            DirectionCase {
                input: Some("FORWARD"),
                expected: LokiDirection::Forward,
            },
            DirectionCase {
                input: Some("FoRwArD"),
                expected: LokiDirection::Forward,
            },
        ] {
            assert!(loki_direction(case.input).unwrap() == case.expected);
        }
        for input in ["sideways", "forward ", " backward", "0", " "] {
            assert!(matches!(
                loki_direction(Some(input)),
                Err(HttpQueryError::InvalidDirection(value)) if value == input
            ));
        }
    }
}

pub(crate) fn parse_decimal_seconds_timestamp(value: &str) -> Option<i64> {
    let negative = value.starts_with('-');
    let unsigned = value
        .strip_prefix('-')
        .or_else(|| value.strip_prefix('+'))
        .unwrap_or(value);
    let separator = unsigned.find('.')?;
    let seconds = &unsigned[..separator];
    let fraction = &unsigned[separator + 1..];
    if seconds.is_empty() && fraction.is_empty() {
        return None;
    }
    if !seconds.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }

    let seconds = if seconds.is_empty() {
        0
    } else {
        seconds.parse::<i128>().ok()?
    };
    let mut fraction_ns = 0_i128;
    let mut scale = 100_000_000_i128;
    for digit in fraction.bytes().take(9) {
        fraction_ns += i128::from(digit - b'0') * scale;
        scale /= 10;
    }

    let timestamp_ns = seconds
        .checked_mul(1_000_000_000)?
        .checked_add(fraction_ns)?;
    let timestamp_ns = if negative {
        timestamp_ns.checked_neg()?
    } else {
        timestamp_ns
    };
    i64::try_from(timestamp_ns).ok()
}

#[derive(Clone, Copy)]
pub(crate) enum QueryKind {
    Instant,
    Range,
}

impl QueryKind {
    /// The route label that query metrics record this kind of query under.
    pub(crate) fn route_label(self) -> &'static str {
        match self {
            Self::Instant => "query",
            Self::Range => "query_range",
        }
    }
}

pub(crate) fn time_range(
    params: &QueryParams,
    kind: QueryKind,
) -> Result<TimeRange, HttpQueryError> {
    match kind {
        QueryKind::Instant => {
            if let Some(time) = params.time {
                TimeRange::new(time, time).map_err(HttpQueryError::from)
            } else if params.start.is_none() && params.since.is_none() && params.end.is_none() {
                // Loki evaluates an instant query with no `time` at the moment it
                // receives it. An unbounded window here put a vector sample at
                // `i64::MAX` instead.
                let now = current_unix_time_ns();
                TimeRange::new(now, now).map_err(HttpQueryError::from)
            } else {
                optional_start_end_range(params.start, params.since, params.end)
            }
        }
        QueryKind::Range => {
            let end = params.end.unwrap_or_else(current_unix_time_ns);
            let start = start_or_since(params.start, params.since, Some(end))?
                .unwrap_or_else(|| end.saturating_sub(LOKI_DEFAULT_QUERY_RANGE.nanos_i64()));
            TimeRange::new(start, end).map_err(HttpQueryError::from)
        }
    }
}
