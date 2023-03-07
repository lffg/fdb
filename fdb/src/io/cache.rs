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

    /// Tries to get the element using the given key. If such an element doesn't
    /// exist, executes the loader future to populate the cache entry.
    pub async fn get_or_load<F, E>(&self, key: K, loader: F) -> Result<Arc<V>, E>
    where
        F: Future<Output = Result<V, E>>,
        E: Clone + Send + Sync + 'static,
    {
        self.inner
            .try_get_with(key, async { loader.await.map(Arc::new) })
            .await
            .map_err(|err| (*err).clone())
    }

    /// Inserts the given key on the cache. Panics if the key was already
    /// defined.
    pub async fn insert_new(&self, key: K, val: Arc<V>)
    where
        K: std::fmt::Debug,
    {
        if self.inner.contains_key(&key) {
            panic!("can't insert key already registered: {key:?}");
        }
        self.inner.insert(key, val).await;
    }

    /// Tries to load the element using the given key.
    pub async fn get(&self, key: &K) -> Option<Arc<V>> {
        self.inner.get(key)
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
            .get_or_load(1, async { Ok::<_, ()>("one".into()) })
            .await
            .unwrap();
        assert_eq!(*v1, "one");

        let v1 = c
            .get_or_load::<_, ()>(1, async {
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
            .get_or_load(1, async { Ok::<_, ()>("one".into()) })
            .await
            .unwrap();
        assert_eq!(*v1, "one");

        let v1_2 = c.get(&1).await.unwrap();
        assert_eq!(&*v1_2, "one");

        c.evict(&1).await;

        assert!(c.get(&1).await.is_none());

        let v1 = c
            .get_or_load(1, async { Ok::<_, ()>("two".into()) })
            .await
            .unwrap();
        assert_eq!(*v1, "two");

        let v1_2 = c.get(&1).await.unwrap();
        assert_eq!(&*v1_2, "two");
    }

    #[tokio::test]
    async fn test_insert_get() {
        let c = build_cache(4);

        assert!(c.get(&1).await.is_none());

        c.insert_new(1, Arc::new("one".into())).await;

        let v1_2 = c.get(&1).await.unwrap();
        assert_eq!(&*v1_2, "one");
    }

    #[tokio::test]
    #[should_panic(expected = "can't insert key already registered")]
    async fn test_insert_not_new() {
        let c = build_cache(4);

        c.insert_new(1, Arc::new("one".into())).await;
        c.insert_new(1, Arc::new("one".into())).await; // BAM!
    }

    fn build_cache(cap: u64) -> Cache<u32, String> {
        Cache::new(cap, RandomState::default())
    }
}
