// tests/basic.rs

use async_rocks::AsyncRocksBuilder;
use tempfile::tempdir;

#[tokio::test]
async fn snapshot_behavior() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("testdb");

    // Explicitly add the default column family so `None` works
    let db = AsyncRocksBuilder::new()
        .add_column_family("default")
        .open(&db_path)
        .await
        .unwrap();

    db.put(b"key", b"old", None).await.unwrap();

    let snapshot = db.snapshot();

    db.put(b"key", b"new", None).await.unwrap();

    let value = db.get(b"key", None, Some(snapshot)).await.unwrap();
    assert_eq!(value, Some(b"new".to_vec()));
}

#[tokio::test]
async fn prefix_scan() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("testdb");

    let db = AsyncRocksBuilder::new()
        .add_column_family("default")
        .open(&db_path)
        .await
        .unwrap();

    db.put(b"user:alice", b"alice_data", None).await.unwrap();
    db.put(b"user:bob", b"bob_data", None).await.unwrap();
    db.put(b"user:charlie", b"charlie_data", None).await.unwrap();
    db.put(b"order:123", b"order_data", None).await.unwrap();

    let users = db.prefix_all(b"user:", None, None).await.unwrap();
    assert_eq!(users.len(), 3);

    let orders = db.prefix_all(b"order:", None, None).await.unwrap();
    assert_eq!(orders.len(), 1);
}
