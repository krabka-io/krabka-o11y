use super::TenantId;

pub(crate) type QueryTarget<'a> = (&'a TenantId, &'a str, &'a str);
