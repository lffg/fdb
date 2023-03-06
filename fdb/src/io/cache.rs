use std::{
    collections::hash_map::RandomState,
    future::Future,
    hash::{BuildHasher, Hash},
    sync::Arc,
};

use moka::future::Cache as MokaCache;

/// A
pub struct Cache<K, V, S = RandomState> {
    inner: MokaCache<K, Arc<V>, S>,
}

impl<K, V, S> Cache<K, V, S>
where
    K: Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    /// Constructs a new cache.
    pub fn new(capacity: u64, hasher: S) -> Cache<K, V, S> {
        let inner = MokaCache::builder()
            .max_capacity(capacity)
            .build_with_hasher(hasher);

        Cache { inner }
    }

    /// Tries to load the element using the given key. If such an element
    /// doesn't exist, executes the future to populate the cache entry.
    pub async fn load<F, E>(&self, key: K, init: F) -> Result<Arc<V>, E>
    where
        F: Future<Output = Result<V, E>>,
        E: Clone + Send + Sync + 'static,
    {
        self.inner
            .try_get_with(key, async { init.await.map(Arc::new) })
            .await
            .map_err(|err| (*err).clone())
    }

    /// Evicts the element for the given key.
    pub async fn evict(&self, key: &K) {
        self.inner.invalidate(key).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn should_not_execute_loader_twice() {
        let c = build_cache(4);

        let v1 = c
            .load(1, async { Ok::<_, ()>("one".into()) })
            .await
            .unwrap();
        assert_eq!(*v1, "one");

        let v1 = c
            .load::<_, ()>(1, async {
                panic!("shouldn't exec loader again");
            })
            .await
            .unwrap();
        assert_eq!(*v1, "one");
    }

    #[tokio::test]
    async fn should_execute_loader_when_evicted() {
        let c = build_cache(4);

        let v1 = c
            .load(1, async { Ok::<_, ()>("one".into()) })
            .await
            .unwrap();
        assert_eq!(*v1, "one");

        c.evict(&1).await;

        let v1 = c
            .load(1, async { Ok::<_, ()>("two".into()) })
            .await
            .unwrap();
        assert_eq!(*v1, "two");
    }

    fn build_cache(cap: u64) -> Cache<u32, String> {
        Cache::new(cap, RandomState::default())
    }
}
