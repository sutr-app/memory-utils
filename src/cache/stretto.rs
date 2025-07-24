use crate::lock::RwLockWithKey;
use anyhow::Result;
use async_trait::async_trait;
use debug_stub_derive::DebugStub;
use futures::Future;
use std::fmt::Debug;
use std::sync::Arc;
use std::{hash::Hash, time::Duration};
use stretto::AsyncCache;

#[derive(serde_derive::Deserialize, Debug, Clone, Copy)]
pub struct MemoryCacheConfig {
    pub num_counters: usize,
    /// max number of items (this cache is used with cost as fixed value 1 in inserting)
    pub max_cost: i64,
    pub use_metrics: bool,
}
impl Default for MemoryCacheConfig {
    fn default() -> Self {
        tracing::info!("Use default memoryCacheConfig.");
        // TODO consider of machine memory size?
        Self {
            num_counters: 12960,
            max_cost: 12960,
            use_metrics: true,
        }
    }
}

// TODO use ByteKey as key ?
// TODO cost
#[async_trait]
pub trait UseMemoryCache<KEY, VAL>
where
    Self: Send + Sync,
    KEY: Hash + Eq + Sync + Send + Debug + Clone + 'static,
    VAL: Debug + Send + Sync + Clone + 'static,
{
    // type KEY: Hash + Eq + Sync + Send + Debug + Clone;
    // type VAL: Debug + Send + Sync + Clone + 'static;

    /// default cache ttl
    fn default_ttl(&self) -> Option<&Duration>;

    fn cache(&self) -> &AsyncCache<KEY, VAL>;

    // lock for anti-stampede
    fn key_lock(&self) -> &RwLockWithKey<KEY>;

    #[inline]
    async fn set_cache(&self, key: KEY, value: VAL, ttl: Option<&Duration>) -> bool
    where
        Self: Send + Sync,
    {
        match ttl.or(self.default_ttl()) {
            Some(t) => self.cache().insert_with_ttl(key, value, 1i64, *t).await,
            None => self.cache().insert(key, value, 1i64).await,
        }
    }

    async fn set_and_wait_cache(&self, key: KEY, value: VAL, ttl: Option<&Duration>) -> bool
    where
        Self: Send + Sync,
    {
        let r = self.set_cache(key, value, ttl).await;
        self.cache()
            .wait()
            .await
            .inspect_err(|e| tracing::warn!("cache wait error?: err: {}", e))
            .unwrap_or(()); // XXX ignore error
        r
    }

    // with concurrent lock (anti-stampede) compatible
    async fn set_and_wait_cache_locked(&self, key: KEY, value: VAL, ttl: Option<&Duration>) -> bool
    where
        Self: Send + Sync,
    {
        let _lock = self.key_lock().write(key.clone()).await;
        let r = self.set_cache(key, value, ttl).await;
        self.cache()
            .wait()
            .await
            .inspect_err(|e| tracing::warn!("cache wait error?: err: {}", e))
            .unwrap_or(()); // XXX ignore error
        r
    }

    async fn wait_cache(&self) {
        self.cache()
            .wait()
            .await
            .inspect_err(|e| tracing::warn!("cache wait error?: err: {}", e))
            .unwrap_or(()); // XXX ignore error
    }
    async fn with_cache_if_some<R, F>(
        &self,
        key: &KEY,
        ttl: Option<&Duration>,
        not_found_case: F,
    ) -> Result<Option<VAL>>
    where
        Self: Send + Sync,
        R: Future<Output = Result<Option<VAL>>> + Send,
        F: FnOnce() -> R + Send,
    {
        match self.find_cache(key).await {
            Some(r) => Ok(Some(r)),
            None => {
                tracing::trace!(
                    "memory cache not found: {:?}, create by not_found_case",
                    key
                );
                let v = not_found_case().await;
                match v {
                    Ok(Some(r)) => {
                        self.set_and_wait_cache(
                            (*key).clone(),
                            r.clone(),
                            ttl.or(self.default_ttl()),
                        )
                        .await;
                        Ok(Some(r))
                    }
                    Ok(None) => Ok(None),
                    Err(e) => {
                        tracing::warn!("cache error: key={:?}, err: {:?}", key, e);
                        Err(e)
                    }
                }
            }
        }
    }

    // with concurrent lock (anti-stampede)
    async fn with_cache_locked<R, F>(
        &self,
        key: &KEY,
        ttl: Option<&Duration>,
        not_found_case: F,
    ) -> Result<VAL>
    where
        Self: Send + Sync,
        R: Future<Output = Result<VAL>> + Send,
        F: FnOnce() -> R + Send,
    {
        let _lock = self.key_lock().write(key.clone()).await;
        match self.find_cache(key).await {
            Some(r) => Ok(r),
            None => {
                tracing::trace!(
                    "memory cache not found: {:?}, create by not_found_case",
                    key
                );
                let v = not_found_case().await;
                match v {
                    Ok(r) => {
                        self.set_and_wait_cache(
                            (*key).clone(),
                            r.clone(),
                            ttl.or(self.default_ttl()),
                        )
                        .await;
                        Ok(r)
                    }
                    Err(e) => {
                        tracing::warn!("cache error: key={:?}, err: {:?}", key, e);
                        Err(e)
                    }
                }
            }
        }
    }

    async fn with_cache<R, F>(
        &self,
        key: &KEY,
        ttl: Option<&Duration>,
        not_found_case: F,
    ) -> Result<VAL>
    where
        Self: Send + Sync,
        R: Future<Output = Result<VAL>> + Send,
        F: FnOnce() -> R + Send,
    {
        match self.find_cache(key).await {
            Some(r) => Ok(r),
            None => {
                tracing::trace!(
                    "memory cache not found: {:?}, create by not_found_case",
                    key
                );
                let v = not_found_case().await;
                match v {
                    Ok(r) => {
                        self.set_and_wait_cache(
                            (*key).clone(),
                            r.clone(),
                            ttl.or(self.default_ttl()),
                        )
                        .await;
                        Ok(r)
                    }
                    Err(e) => {
                        tracing::warn!("cache error: key={:?}, err: {:?}", key, e);
                        Err(e)
                    }
                }
            }
        }
    }

    async fn find_cache_locked(&self, key: &KEY) -> Option<VAL>
    where
        Self: Send + Sync,
    {
        let _lock = self.key_lock().read(key.clone()).await;
        self.cache().get(key).await.map(|v| {
            let res = v.value().clone();
            tracing::trace!("memory cache found: {:?}", key);
            v.release();
            res
        })
    }

    async fn find_cache(&self, key: &KEY) -> Option<VAL>
    where
        Self: Send + Sync,
    {
        self.cache().get(key).await.map(|v| {
            let res = v.value().clone();
            tracing::trace!("memory cache found: {:?}", key);
            v.release();
            res
        })
    }

    async fn delete_cache(&self, key: &KEY) -> Result<()>
    where
        Self: Send + Sync,
    {
        self.cache().try_remove(key).await.map_err(|e| e.into())
    }
    async fn delete_cache_locked(&self, key: &KEY) -> Result<()>
    where
        Self: Send + Sync,
    {
        let _lock = self.key_lock().write(key.clone()).await;
        self.cache().try_remove(key).await.map_err(|e| e.into())
    }
    async fn clear(&self) -> Result<()>
    where
        Self: Send + Sync,
    {
        self.key_lock().clean().await;
        self.cache().clear().await.map_err(|e| e.into())
    }
}

pub fn new_memory_cache<K: Hash + Eq + std::fmt::Debug + Send + Clone, V: Send + Sync + 'static>(
    config: &MemoryCacheConfig,
) -> AsyncCache<K, V> {
    AsyncCache::<K, V>::builder(config.num_counters, config.max_cost)
        .set_metrics(config.use_metrics)
        .finalize(tokio::spawn)
        .unwrap()
}

#[derive(DebugStub, Clone)]
pub struct MemoryCacheImpl<K: Hash + Eq + std::fmt::Debug + Send + Clone, V: Send + Sync + 'static>
{
    #[debug_stub = "AsyncCache<K, V>"]
    cache: AsyncCache<K, V>,
    key_lock: Arc<RwLockWithKey<K>>,
    default_ttl: Option<Duration>,
}
impl<K: Hash + Eq + Send + Clone, V: Send + Sync + 'static> UseMemoryCache<K, V>
    for MemoryCacheImpl<K, V>
where
    K: Hash + Eq + Send + Sync + Clone + Debug + 'static,
    V: Debug + Send + Sync + Clone + 'static,
{
    fn default_ttl(&self) -> Option<&Duration> {
        self.default_ttl.as_ref()
    }
    fn cache(&self) -> &AsyncCache<K, V> {
        &self.cache
    }
    fn key_lock(&self) -> &RwLockWithKey<K> {
        &self.key_lock
    }
}

impl<K: Hash + Eq + std::fmt::Debug + Send + Clone, V: Send + Sync + 'static>
    MemoryCacheImpl<K, V>
{
    pub fn new(config: &MemoryCacheConfig, default_ttl: Option<Duration>) -> Self {
        Self {
            cache: new_memory_cache::<K, V>(config),
            key_lock: Arc::new(RwLockWithKey::new(config.num_counters)),
            default_ttl,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::cache::stretto::{MemoryCacheConfig, MemoryCacheImpl, UseMemoryCache};
    use anyhow::Result;
    use std::{sync::Arc, time::Duration};

    #[tokio::test]
    async fn with_cache_test() {
        let config = MemoryCacheConfig {
            num_counters: 10000,
            max_cost: 1e6 as i64,
            use_metrics: true,
        };
        let cache = MemoryCacheImpl::new(&config, Some(Duration::from_secs(60)));

        let key = "hoge";
        let value1 = "value!!";
        let value2 = "value2!!";
        let ttl = Some(Duration::from_secs_f32(0.2));
        // let ttl = None; // Some(Duration::from_secs(1));
        // resolve and store cache
        let val1: Result<&str> = cache
            .with_cache(&key, ttl.as_ref(), move || async move { Ok(value1) })
            .await;
        assert_eq!(val1.unwrap(), value1);
        // use cache
        let val2: Result<&str> = cache
            .with_cache(&key, ttl.as_ref(), move || async move { Ok(value2) })
            .await;
        assert_eq!(val2.unwrap(), value1);
        // cache expired
        tokio::time::sleep(Duration::from_millis(200)).await;
        let val3: Result<&str> = cache
            .with_cache(&key, ttl.as_ref(), move || async move { Ok(value2) })
            .await;
        assert_eq!(val3.unwrap(), value2);
        let val4: Option<&str> = cache.find_cache(&key).await;
        assert_eq!(val4.unwrap(), value2);
    }

    #[tokio::test]
    async fn with_arc_key_cache_test() {
        let config = MemoryCacheConfig {
            num_counters: 10000,
            max_cost: 1e6 as i64,
            use_metrics: true,
        };
        let cache = MemoryCacheImpl::new(&config, Some(Duration::from_secs(60)));
        let key = &Arc::new(String::from("hoge"));
        let value1 = "value!!";
        let value2 = "value2!!";
        let ttl = Some(Duration::from_secs_f32(0.2));
        // resolve and store cache
        assert_eq!(None, cache.find_cache(&key.clone()).await);
        let val1: Result<&str> = cache
            .with_cache(key, ttl.as_ref(), move || async move { Ok(value1) })
            .await;
        assert_eq!(val1.unwrap(), value1);
        // use cache
        assert_eq!(Some(value1), cache.find_cache(&key.clone()).await);
        let val2: Result<&str> = cache
            .with_cache(key, ttl.as_ref(), move || async move { Ok(value2) })
            .await;
        assert_eq!(val2.unwrap(), value1);
        // cache expired
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(None, cache.find_cache(&key.clone()).await);
        let val3: Result<&str> = cache
            .with_cache(key, None, move || async move { Ok(value2) })
            .await;
        assert_eq!(val3.unwrap(), value2);
        assert_eq!(Some(value2), cache.find_cache(key).await);
    }
    #[tokio::test]
    async fn set_find_cache_test() {
        let config = MemoryCacheConfig {
            num_counters: 10000,
            max_cost: 1e6 as i64,
            use_metrics: true,
        };
        let cache = MemoryCacheImpl::new(&config, Some(Duration::from_secs(60)));

        let key = "hoge";
        let value1 = "value!!";
        let value2 = "value2!!";
        let ttl = Some(Duration::from_secs_f32(0.2));
        // let ttl = None; // Some(Duration::from_secs(1));
        // resolve and store cache
        assert!(cache.set_cache(key, value1, ttl.as_ref()).await);
        cache.wait_cache().await;
        assert_eq!(cache.find_cache(&key).await.unwrap(), value1);
        // use cache
        assert!(cache.set_cache(key, value2, ttl.as_ref()).await);
        cache.wait_cache().await;
        assert_eq!(cache.find_cache(&key).await.unwrap(), value2);
        // cache expired
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(cache.find_cache(&key).await, None);
    }
    // multi-threaded with-cache test
}
