//! In-memory cache layer in front of `SQLite` persistence.
//!
//! Query handlers may read through this cache; command handlers invalidate entries
//! after successful writes.

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::hash::Hash;
use std::time::Duration;

#[derive(Clone)]
struct CacheEntry<V> {
    value: V,
    expires_at: Option<DateTime<Utc>>,
}

/// Thread-safe TTL cache for query snapshots and hot read paths.
pub struct CacheLayer<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    store: RwLock<HashMap<K, CacheEntry<V>>>,
    default_ttl: Option<Duration>,
}

impl<K, V> CacheLayer<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    #[must_use]
    pub fn new(default_ttl: Option<Duration>) -> Self {
        Self {
            store: RwLock::new(HashMap::new()),
            default_ttl,
        }
    }

    #[must_use]
    pub fn get(&self, key: &K) -> Option<V> {
        let store = self.store.read();
        let entry = store.get(key)?;
        let value = entry.value.clone();
        let expires_at = entry.expires_at;
        drop(store);

        if let Some(expires_at) = expires_at {
            if Utc::now() > expires_at {
                return None;
            }
        }
        Some(value)
    }

    pub fn insert(&self, key: K, value: V) {
        self.insert_with_ttl(key, value, self.default_ttl);
    }

    pub fn insert_with_ttl(&self, key: K, value: V, ttl: Option<Duration>) {
        let expires_at =
            ttl.and_then(|d| Utc::now().checked_add_signed(chrono::Duration::from_std(d).ok()?));
        self.store
            .write()
            .insert(key, CacheEntry { value, expires_at });
    }

    pub fn invalidate(&self, key: &K) {
        self.store.write().remove(key);
    }

    pub fn clear(&self) {
        self.store.write().clear();
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.store.read().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.store.read().is_empty()
    }
}

impl<K, V> Default for CacheLayer<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn default() -> Self {
        Self::new(Some(Duration::from_mins(1)))
    }
}
