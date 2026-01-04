//! # async-rocksdb
//!
//! An ergonomic, async wrapper for RocksDB in Rust.
//!
//! Provides non-blocking operations suitable for Tokio-based applications.
//!
//! ## Features
//!
//! - Async reads/writes
//! - Column families
//! - Snapshots
//! - Prefix scans
//! - Batch operations
//! - Manual flush/compaction

// src/lib.rs

use rocksdb::{
    DB, Options, BlockBasedOptions, Cache,
    ColumnFamilyDescriptor, DBCompressionType, IteratorMode, ReadOptions,
};
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;
use tokio::task;

/// Error type for async-rocksdb operations.
#[derive(Error, Debug)]
pub enum AsyncRocksError {
    #[error("RocksDB error: {0}")]
    Rocks(#[from] rocksdb::Error),
    #[error("Task failed: {0}")]
    Join(#[from] task::JoinError),
    #[error("Column family not found: {0}")]
    ColumnFamilyNotFound(String),
}

/// Configuration for a single column family.
pub struct ColumnFamilyConfig {
    name: String,
    options: Options,
}

impl ColumnFamilyConfig {
    /// Creates a new column family configuration with the given name.
    /// 
    /// The column family will be created if it doesn't exist when the database is opened.
    pub fn new(name: &str) -> Self {
        let mut options = Options::default();
        options.create_if_missing(true);
        Self {
            name: name.to_string(),
            options,
        }
    }

    /// Sets the compression type for this column family.
    pub fn compression(mut self, compression: DBCompressionType) -> Self {
        self.options.set_compression_type(compression);
        self
    }

    /// Sets the block cache size for this column family.
    pub fn block_cache_size(mut self, size_bytes: usize) -> Self {
        let cache = Cache::new_lru_cache(size_bytes);
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_cache(&cache);
        self.options.set_block_based_table_factory(&block_opts);
        self
    }
}

/// Builder for configuring and opening an [`AsyncRocksDB`] instance.
pub struct AsyncRocksBuilder {
    db_options: Options,
    column_families: Vec<ColumnFamilyConfig>,
}

impl Default for AsyncRocksBuilder {
    fn default() -> Self {
        let mut db_options = Options::default();
        db_options.create_if_missing(true);
        Self {
            db_options,
            column_families: vec![],
        }
    }
}

impl AsyncRocksBuilder {
    /// Creates a new builder with default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a column family with the given name and default options.
    pub fn add_column_family(mut self, name: &str) -> Self {
        self.column_families.push(ColumnFamilyConfig::new(name));
        self
    }

    /// Opens the database at the given path with the configured options.
    pub async fn open<P: AsRef<Path>>(mut self, path: P) -> Result<AsyncRocksDB, AsyncRocksError> {
        let path = path.as_ref().to_path_buf();

        self.db_options.create_if_missing(true);

        if !self.column_families.is_empty() {
            self.db_options.create_missing_column_families(true);
        }

        let db = if self.column_families.is_empty() {
            task::spawn_blocking(move || DB::open(&self.db_options, &path)).await??
        } else {
            let cf_descriptors: Vec<_> = self.column_families
                .into_iter()
                .map(|cf| ColumnFamilyDescriptor::new(cf.name, cf.options))
                .collect();

            task::spawn_blocking(move || {
                DB::open_cf_descriptors(&self.db_options, &path, cf_descriptors)
            })
            .await??
        };

        Ok(AsyncRocksDB {
            inner: Arc::new(db),
        })
    }
}

/// A point-in-time snapshot of the database.
///
/// Snapshots provide a consistent view of the data at the time they were created.
/// In an async context with `spawn_blocking`, reads using a snapshot may see later writes
/// due to scheduling, but they offer best-effort consistency.
#[derive(Clone)]
pub struct Snapshot {
    db: Arc<DB>,
}

impl Snapshot {
    fn new(db: Arc<DB>) -> Self {
        Self { db }
    }
}

/// Async-aware wrapper for RocksDB.
///
/// Provides non-blocking operations suitable for use in Tokio-based applications.
/// All operations are executed via `tokio::task::spawn_blocking` to avoid blocking the async runtime.
pub struct AsyncRocksDB {
    inner: Arc<DB>,
}

impl AsyncRocksDB {
    /// Opens a database with default options at the given path.
    ///
    /// Equivalent to `AsyncRocksBuilder::new().open(path)`.
    pub async fn open_default<P: AsRef<Path>>(path: P) -> Result<Self, AsyncRocksError> {
        AsyncRocksBuilder::new().open(path).await
    }

    /// Creates a new snapshot of the current database state.
    pub fn snapshot(&self) -> Snapshot {
        Snapshot::new(self.inner.clone())
    }

    /// Inserts a key-value pair into the database.
    ///
    /// `cf` specifies the column family. Use `None` for the default column family.
    pub async fn put<K, V>(
        &self,
        key: K,
        value: V,
        cf: Option<&str>,
    ) -> Result<(), AsyncRocksError>
    where
        K: AsRef<[u8]> + Send + 'static,
        V: AsRef<[u8]> + Send + 'static,
    {
        let db = self.inner.clone();
        let key = key.as_ref().to_vec();
        let value = value.as_ref().to_vec();
        let cf_name = cf.map(|s| s.to_string());

        task::spawn_blocking(move || {
            let cf_name = cf_name.as_deref().unwrap_or("default");
            let cf = db.cf_handle(cf_name)
                .ok_or(AsyncRocksError::ColumnFamilyNotFound(cf_name.to_string()))?;
            db.put_cf(cf, key, value)?;
            Ok(())
        })
        .await?
    }

    /// Retrieves a value by key.
    ///
    /// Returns `None` if the key does not exist.
    /// Use `snapshot` for reading from a point-in-time view.
    pub async fn get<K>(
        &self,
        key: K,
        cf: Option<&str>,
        snapshot: Option<Snapshot>,
    ) -> Result<Option<Vec<u8>>, AsyncRocksError>
    where
        K: AsRef<[u8]> + Send + 'static,
    {
        let db = self.inner.clone();
        let key = key.as_ref().to_vec();
        let cf_name = cf.map(|s| s.to_string());

        task::spawn_blocking(move || {
            let cf_name = cf_name.as_deref().unwrap_or("default");
            let cf = db.cf_handle(cf_name)
                .ok_or(AsyncRocksError::ColumnFamilyNotFound(cf_name.to_string()))?;

            if let Some(snap) = snapshot {
                let raw_snap = snap.db.snapshot();
                let mut opts = ReadOptions::default();
                opts.set_snapshot(&raw_snap);
                db.get_cf_opt(cf, &key, &opts)
            } else {
                db.get_cf(cf, &key)
            }
            .map_err(Into::into)
        })
        .await?
    }

    /// Deletes a key from the database.
    pub async fn delete<K>(
        &self,
        key: K,
        cf: Option<&str>,
    ) -> Result<(), AsyncRocksError>
    where
        K: AsRef<[u8]> + Send + 'static,
    {
        let db = self.inner.clone();
        let key = key.as_ref().to_vec();
        let cf_name = cf.map(|s| s.to_string());

        task::spawn_blocking(move || {
            let cf_name = cf_name.as_deref().unwrap_or("default");
            let cf = db.cf_handle(cf_name)
                .ok_or(AsyncRocksError::ColumnFamilyNotFound(cf_name.to_string()))?;
            db.delete_cf(cf, key)?;
            Ok(())
        })
        .await?
    }

    /// Retrieves multiple values by keys in a single operation.
    ///
    /// Returns a `Vec` with the same length as `keys`, containing `Some(value)` or `None`.
    pub async fn multi_get<K>(
        &self,
        keys: Vec<K>,
        cf: Option<&str>,
        snapshot: Option<Snapshot>,
    ) -> Result<Vec<Option<Vec<u8>>>, AsyncRocksError>
    where
        K: AsRef<[u8]> + Send + 'static,
    {
        let db = self.inner.clone();
        let keys: Vec<Vec<u8>> = keys.into_iter().map(|k| k.as_ref().to_vec()).collect();
        let cf_name = cf.map(|s| s.to_string());

        task::spawn_blocking(move || {
            let cf_name = cf_name.as_deref().unwrap_or("default");
            let cf = db.cf_handle(cf_name)
                .ok_or(AsyncRocksError::ColumnFamilyNotFound(cf_name.to_string()))?;

            if let Some(snap) = snapshot {
                let raw_snap = snap.db.snapshot();
                let mut opts = ReadOptions::default();
                opts.set_snapshot(&raw_snap);
                db.multi_get_cf_opt(keys.iter().map(|k| (&cf, k)), &opts)
            } else {
                db.multi_get_cf(keys.iter().map(|k| (&cf, k)))
            }
            .into_iter()
            .map(|r| r.map_err(Into::into))
            .collect()
        })
        .await?
    }

    /// Deletes multiple keys in a single operation.
    pub async fn multi_delete<K>(
        &self,
        keys: Vec<K>,
        cf: Option<&str>,
    ) -> Result<(), AsyncRocksError>
    where
        K: AsRef<[u8]> + Send + 'static,
    {
        let db = self.inner.clone();
        let keys: Vec<Vec<u8>> = keys.into_iter().map(|k| k.as_ref().to_vec()).collect();
        let cf_name = cf.map(|s| s.to_string());

        task::spawn_blocking(move || {
            let mut batch = rocksdb::WriteBatch::default();
            let cf_name = cf_name.as_deref().unwrap_or("default");
            let cf = db.cf_handle(cf_name)
                .ok_or(AsyncRocksError::ColumnFamilyNotFound(cf_name.to_string()))?;

            for key in keys {
                batch.delete_cf(&cf, &key);
            }
            db.write(batch)?;
            Ok(())
        })
        .await?
    }

    /// Forces a flush of memtables to SST files for the entire database.
    pub async fn flush(&self) -> Result<(), AsyncRocksError> {
        let db = self.inner.clone();
        task::spawn_blocking(move || db.flush()).await??;
        Ok(())
    }

    /// Compacts a range of keys in the specified column family.
    pub async fn compact_range<K: AsRef<[u8]>>(
        &self,
        start: Option<K>,
        end: Option<K>,
        cf: Option<&str>,
    ) -> Result<(), AsyncRocksError> {
        let db = self.inner.clone();
        let start = start.map(|k| k.as_ref().to_vec());
        let end = end.map(|k| k.as_ref().to_vec());
        let cf_name = cf.map(|s| s.to_string());

        task::spawn_blocking(move || {
            let cf_name = cf_name.as_deref().unwrap_or("default");
            let cf = db.cf_handle(cf_name)
                .ok_or(AsyncRocksError::ColumnFamilyNotFound(cf_name.to_string()))?;
            db.compact_range_cf(&cf, start.as_deref(), end.as_deref());
            Ok(())
        })
        .await?
    }

    /// Returns all key-value pairs in the specified column family.
    pub async fn all(
        &self,
        cf: Option<&str>,
        snapshot: Option<Snapshot>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, AsyncRocksError> {
        let db = self.inner.clone();
        let cf_name = cf.map(|s| s.to_string());

        task::spawn_blocking(move || {
            let cf_name = cf_name.as_deref().unwrap_or("default");
            let cf = db.cf_handle(cf_name)
                .ok_or(AsyncRocksError::ColumnFamilyNotFound(cf_name.to_string()))?;

            let iter = if let Some(snap) = snapshot {
                let raw_snap = snap.db.snapshot();
                let mut opts = ReadOptions::default();
                opts.set_snapshot(&raw_snap);
                db.iterator_cf_opt(cf, opts, IteratorMode::Start)
            } else {
                db.iterator_cf(cf, IteratorMode::Start)
            };

            iter.map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())))
                .collect::<Result<Vec<_>, _>>()
                .map_err(Into::into)
        })
        .await?
    }

    /// Returns all key-value pairs matching the given prefix.
    ///
    /// Uses RocksDB's prefix seek optimization for high performance.
    pub async fn prefix_all<P>(
        &self,
        prefix: P,
        cf: Option<&str>,
        snapshot: Option<Snapshot>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, AsyncRocksError>
    where
        P: AsRef<[u8]> + Send + 'static,
    {
        let db = self.inner.clone();
        let prefix = prefix.as_ref().to_vec();
        let cf_name = cf.map(|s| s.to_string());

        task::spawn_blocking(move || {
            let cf_name = cf_name.as_deref().unwrap_or("default");
            let cf = db.cf_handle(cf_name)
                .ok_or(AsyncRocksError::ColumnFamilyNotFound(cf_name.to_string()))?;

            let mut opts = ReadOptions::default();
            opts.set_prefix_same_as_start(true);

            if let Some(snap) = snapshot {
                let raw_snap = snap.db.snapshot();
                opts.set_snapshot(&raw_snap);
            }

            let iter = db.iterator_cf_opt(
                cf,
                opts,
                IteratorMode::From(&prefix, rocksdb::Direction::Forward),
            );

            iter.take_while(|r| {
                r.as_ref()
                    .map(|(k, _)| k.starts_with(&prefix))
                    .unwrap_or(false)
            })
            .map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())))
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
        })
        .await?
    }
}
