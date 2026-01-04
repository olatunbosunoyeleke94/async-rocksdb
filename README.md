[![Crates.io](https://img.shields.io/crates/v/async-rocksdb)](https://crates.io/crates/async-rocksdb)
[![Docs.rs](https://docs.rs/async-rocksdb/badge.svg)](https://docs.rs/async-rocksdb)

# async-rocksdb

An ergonomic, async wrapper for RocksDB in Rust.

## Features

- Fully async operations
- Column families with custom options
- Consistent snapshots
- High-performance prefix scans
- Batch reads/deletes (`multi_get`, `multi_delete`)
- Manual flush and compaction

## Example


```rust
use async_rocksdb::AsyncRocksBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = AsyncRocksBuilder::new()
        .open("/tmp/mydb")
        .await?;

    db.put(b"key", b"value", None).await?;
    let val = db.get(b"key", None, None).await?;
    println!("{:?}", val);

    Ok(())
}


## Column Families

```rust
let db = AsyncRocksBuilder::new()
    .add_column_family("users")
    .add_column_family("logs")
    .open("/tmp/mydb")
    .await?;

db.put(b"user:alice", b"data", Some("users")).await?;
let val = db.get(b"user:alice", Some("users"), None).await?;


## Snapshots

Snapshots are best-effort consistent in async context due to spawn_blocking scheduling.

```rust
let snapshot = db.snapshot();
db.put(b"key", b"new", None).await?;
// Latest read sees "new"
assert_eq!(db.get(b"key", None, None).await?, Some(b"new".to_vec()));
// Snapshot read sees old value (or latest in async context)
let old = db.get(b"key", None, Some(snapshot)).await?;


## Prefix Scans

```rust
db.put(b"user:alice", b"...", None).await?;
db.put(b"user:bob", b"...", None).await?;
db.put(b"order:123", b"...", None).await?;

let users = db.prefix_all(b"user:", None, None).await?;
assert_eq!(users.len(), 2);

## Batch Operations

```rust
let keys = vec![b"key1", b"key2"];
let values = db.multi_get(keys, None, None).await?;
let _ = db.multi_delete(vec![b"key1", b"key2"], None).await?;

## Why async-rocksdb?

The official crate for rocksdb is perfect but sync only.
In async apps, calls are wrapped in spawn_blocking.
And this crate does that with a clean API AND extra features like prefix scans and snapshots.
