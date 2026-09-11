use super::*;

/// A read marks a tenant as used, so the tenant removed at the capacity is the
/// one no request touched for longest, not the one inserted first.
#[test]
pub(crate) fn the_tenant_lru_removes_the_least_recently_used_tenant() {
    let mut lru = TenantLru::new(NonZeroUsize::new(2).expect("a nonzero capacity"));
    let (a, b, c) = (
        tenant_for_test("a"),
        tenant_for_test("b"),
        tenant_for_test("c"),
    );
    *lru.get_or_insert_with(&a, || 0) += 1;
    *lru.get_or_insert_with(&b, || 0) += 1;
    check!(lru.get_mut(&a).copied() == Some(1));
    *lru.get_or_insert_with(&c, || 0) += 1;
    *lru.get_or_insert_with(&a, || 0) += 1;

    check!(lru.get_mut(&b).is_none());
    check!(lru.get_mut(&a).copied() == Some(2));
    check!(lru.get_mut(&c).copied() == Some(1));
    check!(lru.entries.len() == 2);
    check!(lru.recency.len() == 2);
}
