use super::lock::RwLockWithKey;
use anyhow::Result;
use async_trait::async_trait;
use debug_stub_derive::DebugStub;
use futures::Future;
use moka::future::Cache;
use moka::ops;
use moka::ops::compute::CompResult;
use std::fmt::Debug;
use std::sync::Arc;
use std::{hash::Hash, time::Duration};

#[derive(serde_derive::Deserialize, Debug, Clone, Copy)]
pub struct MokaCacheConfig {
    pub num_counters: usize,
    pub ttl: Option<Duration>,
}
impl Default for MokaCacheConfig {
    fn default() -> Self {
        tracing::info!("Use default memoryCacheConfig.");
        Self {
            num_counters: 12960,
            ttl: Some(Duration::from_secs(60)),
        }
    }
}

pub type MokaCache<K, V> = Cache<K, V>;
// TODO use ByteKey as key ?
// TODO cost
#[async_trait]
pub trait UseMokaCache<KEY, VAL>
where
    Self: Send + Sync,
    KEY: Hash + Eq + Sync + Send + Debug + Clone + 'static,
    VAL: Debug + Send + Sync + Clone + 'static,
{
    fn cache(&self) -> &Cache<KEY, VAL>;

    #[inline]
    async fn set_cache(&self, key: KEY, value: VAL)
    where
        Self: Send + Sync,
    {
        self.cache().insert(key, value).await
    }

    async fn with_cache_if_some<R, F>(&self, key: &KEY, not_found_case: F) -> Result<Option<VAL>>
    where
        Self: Send + Sync,
        R: Future<Output = Result<Option<VAL>>> + Send,
        F: FnOnce() -> R + Send,
    {
        self.cache()
            .entry(key.clone())
            .and_try_compute_with(|entry| async move {
                if entry.is_some() {
                    Ok(ops::compute::Op::Nop)
                } else {
                    tracing::trace!(
                        "memory cache not found: {:?}, create by not_found_case",
                        key
                    );
                    match not_found_case().await {
                        Ok(Some(r)) => Ok(ops::compute::Op::Put(r)),
                        Ok(None) => {
                            tracing::trace!("value is None: {:?}", key);
                            Ok(ops::compute::Op::Nop)
                        }
                        Err(e) => {
                            tracing::warn!("cache error: key={:?}, err: {:?}", key, e);
                            Err(e)
                        }
                    }
                }
            })
            .await
            .map(|v| match v {
                CompResult::Inserted(entry) => Some(entry.value().clone()),
                CompResult::ReplacedWith(entry) => Some(entry.value().clone()),
                CompResult::StillNone(_) => None,
                CompResult::Unchanged(entry) => Some(entry.value().clone()),
                // XXX should return nothing?
                CompResult::Removed(entry) => Some(entry.value().clone()),
            })
    }

    async fn with_cache<R, F>(&self, key: &KEY, not_found_case: F) -> Result<VAL>
    where
        Self: Send + Sync,
        R: Future<Output = Result<VAL>> + Send,
        F: FnOnce() -> R + Send,
    {
        self.cache()
            .entry(key.clone())
            .and_try_compute_with(|entry| async move {
                if entry.is_some() {
                    Ok(ops::compute::Op::Nop)
                } else {
                    tracing::trace!(
                        "memory cache not found: {:?}, create by not_found_case",
                        key
                    );
                    let v = not_found_case().await;
                    match v {
                        Ok(r) => {
                            // self.set_cache((*key).clone(), r.clone(), ttl.or(self.ttl()))
                            //     .await;
                            Ok(ops::compute::Op::Put(r))
                        }
                        Err(e) => {
                            tracing::warn!("cache error: key={:?}, err: {:?}", key, e);
                            Err(e)
                        }
                    }
                }
            })
            .await
            .and_then(|v| match v {
                CompResult::Inserted(entry) => Ok(entry.value().clone()),
                CompResult::ReplacedWith(entry) => Ok(entry.value().clone()),
                CompResult::StillNone(_) => Err(anyhow::anyhow!("cache not found")),
                CompResult::Unchanged(entry) => Ok(entry.value().clone()),
                // XXX should return nothing?
                CompResult::Removed(entry) => Ok(entry.value().clone()),
            })
    }

    async fn find_cache(&self, key: &KEY) -> Option<VAL>
    where
        Self: Send + Sync,
    {
        self.cache().get(key).await
    }

    async fn delete_cache(&self, key: &KEY) -> Option<VAL>
    where
        Self: Send + Sync,
    {
        self.cache().remove(key).await
    }
    async fn delete_cache_locked(&self, key: &KEY) -> Option<VAL>
    where
        Self: Send + Sync,
    {
        self.cache().remove(key).await
    }
    async fn clear(&self)
    where
        Self: Send + Sync,
    {
        self.cache().invalidate_all();
    }
}

pub fn new_memory_cache<
    K: Hash + Eq + std::fmt::Debug + Send + Sync + Clone + 'static,
    V: Send + Sync + Clone + 'static,
>(
    config: &MokaCacheConfig,
) -> Cache<K, V> {
    let mut b = Cache::builder().max_capacity(config.num_counters as u64);
    if let Some(ttl) = config.ttl {
        b = b.time_to_live(ttl);
    }
    b.build()
}

#[derive(DebugStub, Clone)]
pub struct MokaCacheImpl<K: Hash + Eq + std::fmt::Debug + Send + Clone, V: Send + Sync + 'static> {
    #[debug_stub = "Cache<K, V>"]
    cache: Cache<K, V>,
    key_lock: Arc<RwLockWithKey<K>>,
    ttl: Option<Duration>,
}
impl<K: Hash + Eq + Send + Clone, V: Send + Sync + 'static> UseMokaCache<K, V>
    for MokaCacheImpl<K, V>
where
    K: Hash + Eq + Send + Sync + Clone + Debug + 'static,
    V: Debug + Send + Sync + Clone + 'static,
{
    fn cache(&self) -> &MokaCache<K, V> {
        &self.cache
    }
}

impl<
        K: Hash + Eq + std::fmt::Debug + Send + Sync + Clone + 'static,
        V: Send + Sync + Clone + 'static,
    > MokaCacheImpl<K, V>
{
    pub fn new(config: &MokaCacheConfig) -> Self {
        Self {
            cache: new_memory_cache::<K, V>(config),
            key_lock: Arc::new(RwLockWithKey::new(config.num_counters)),
            ttl: config.ttl,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::infra::cache::{MokaCacheConfig, MokaCacheImpl, UseMokaCache};
    use anyhow::Result;
    use std::{sync::Arc, time::Duration};

    #[tokio::test]
    async fn with_cache_test() {
        let config = MokaCacheConfig {
            num_counters: 10000,
            ttl: Some(Duration::from_millis(200)),
        };
        let cache = MokaCacheImpl::new(&config);

        let key = "hoge";
        let value1 = "value!!";
        let value2 = "value2!!";
        // let ttl = None; // Some(Duration::from_secs(1));
        // resolve and store cache
        let val1: Result<&str> = cache
            .with_cache(&key, move || async move { Ok(value1) })
            .await;
        assert_eq!(val1.unwrap(), value1);
        // use cache
        let val2: Result<&str> = cache
            .with_cache(&key, move || async move { Ok(value2) })
            .await;
        assert_eq!(val2.unwrap(), value1);
        // cache expired
        tokio::time::sleep(Duration::from_millis(200)).await;
        let val3: Result<&str> = cache
            .with_cache(&key, move || async move { Ok(value2) })
            .await;
        assert_eq!(val3.unwrap(), value2);
        let val4: Option<&str> = cache.find_cache(&key).await;
        assert_eq!(val4.unwrap(), value2);
    }

    #[tokio::test]
    async fn with_arc_key_cache_test() {
        let config = MokaCacheConfig {
            num_counters: 10000,
            ttl: Some(Duration::from_millis(200)),
        };
        let cache = MokaCacheImpl::new(&config);
        let key = &Arc::new(String::from("hoge"));
        let value1 = "value!!";
        let value2 = "value2!!";
        // resolve and store cache
        assert_eq!(None, cache.find_cache(&key.clone()).await);
        let val1: Result<&str> = cache
            .with_cache(key, move || async move { Ok(value1) })
            .await;
        assert_eq!(val1.unwrap(), value1);
        // use cache
        assert_eq!(Some(value1), cache.find_cache(&key.clone()).await);
        let val2: Result<&str> = cache
            .with_cache(key, move || async move { Ok(value2) })
            .await;
        assert_eq!(val2.unwrap(), value1);
        // cache expired
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(None, cache.find_cache(&key.clone()).await);
        let val3: Result<&str> = cache
            .with_cache(key, move || async move { Ok(value2) })
            .await;
        assert_eq!(val3.unwrap(), value2);
        assert_eq!(Some(value2), cache.find_cache(key).await);
    }

    #[tokio::test]
    async fn set_find_cache_test() {
        let config = MokaCacheConfig {
            num_counters: 10000,
            ttl: Some(Duration::from_millis(200)),
        };
        let cache = MokaCacheImpl::new(&config);

        let key = "hoge";
        let value1 = "value!!";
        let value2 = "value2!!";
        // let ttl = None; // Some(Duration::from_secs(1));
        // resolve and store cache
        cache.set_cache(key, value1).await;
        assert_eq!(cache.find_cache(&key).await.unwrap(), value1);
        // use cache
        cache.set_cache(key, value2).await;
        assert_eq!(cache.find_cache(&key).await.unwrap(), value2);
        // cache expired
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(cache.find_cache(&key).await, None);
    }
    // multi-threaded with-cache test
}
