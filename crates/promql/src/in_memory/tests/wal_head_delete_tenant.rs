use krabka_blockstore::Labels;

use super::{MetricStore, WalHead};

#[tokio::test]
async fn wal_head_delete_tenant_removes_only_that_tenant() {
    let head = WalHead::new();
    for tenant in ["tenant-a", "tenant-b"] {
        head.update(|store| {
            store.push_float(
                tenant,
                [("__name__".to_owned(), "up".to_owned())]
                    .into_iter()
                    .collect::<Labels>(),
                1_000,
                1.0,
            );
        });
    }

    head.delete_tenant("tenant-a");

    assert2::check!(
        head.series("tenant-a", &[], 0, 2_000)
            .await
            .unwrap()
            .is_empty()
    );
    assert2::check!(head.series("tenant-b", &[], 0, 2_000).await.unwrap().len() == 1);
}
