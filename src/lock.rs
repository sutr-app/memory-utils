use std::{
    collections::HashMap,
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{Mutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, TryLockError};

#[derive(Debug)]
pub struct RwLockWithKey<KEY> {
    locks: Mutex<HashMap<KEY, Arc<RwLock<()>>>>,
    accesses: AtomicUsize,
    capacity: usize,
}

impl<KEY> Default for RwLockWithKey<KEY> {
    fn default() -> Self {
        Self {
            locks: Mutex::default(),
            accesses: AtomicUsize::default(),
            capacity: 1024,
        }
    }
}

impl<K> RwLockWithKey<K>
where
    K: Eq + Hash + Send + Clone,
{
    /// Create new instance of a [KeyRwLock]
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            locks: Mutex::default(),
            accesses: AtomicUsize::default(),
            capacity,
        }
    }

    pub async fn read(&self, key: K) -> OwnedRwLockReadGuard<()> {
        let mut locks = self.locks.lock().await;
        self.recycle_with_count(&mut locks, false).await;

        let lock = locks.entry(key).or_default().clone();
        drop(locks);

        lock.read_owned().await
    }

    pub async fn write(&self, key: K) -> OwnedRwLockWriteGuard<()> {
        let mut locks = self.locks.lock().await;
        self.recycle_with_count(&mut locks, false).await;

        let lock = locks.entry(key).or_default().clone();
        drop(locks);

        lock.write_owned().await
    }

    pub async fn try_read(&self, key: K) -> Result<OwnedRwLockReadGuard<()>, TryLockError> {
        let mut locks = self.locks.lock().await;
        self.recycle_with_count(&mut locks, false).await;

        let lock = locks.entry(key).or_default().clone();
        drop(locks);

        lock.try_read_owned()
    }

    pub async fn try_write(&self, key: K) -> Result<OwnedRwLockWriteGuard<()>, TryLockError> {
        let mut locks = self.locks.lock().await;
        self.recycle_with_count(&mut locks, false).await;

        let lock = locks.entry(key).or_default().clone();
        drop(locks);

        lock.try_write_owned()
    }
    pub async fn clean(&self) {
        let mut locks = self.locks.lock().await;
        Self::_remove_unused(&mut locks)
    }

    #[inline]
    async fn recycle_with_count(&self, locks: &mut HashMap<K, Arc<RwLock<()>>>, force: bool) {
        if force
            || self.accesses.fetch_add(1, Ordering::Relaxed) % self.capacity == self.capacity - 1
        {
            Self::_remove_unused(locks)
        }
    }
    #[inline]
    fn _remove_unused(locks: &mut HashMap<K, Arc<RwLock<()>>>) {
        let mut to_remove = Vec::new();
        for (key, lock) in locks.iter() {
            if lock.try_write().is_ok() {
                to_remove.push(key.clone());
            }
        }
        for key in to_remove {
            locks.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_funcionality() {
        let lock = RwLockWithKey::default();

        let _foo = lock.write("foo").await;
        let _bar = lock.read("bar").await;

        assert!(lock.try_read("foo").await.is_err());
        assert!(lock.try_write("foo").await.is_err());

        assert!(lock.try_read("bar").await.is_ok());
        assert!(lock.try_write("bar").await.is_err());
    }

    #[tokio::test]
    async fn test_clean_up() {
        let lock = RwLockWithKey::default();
        let _foo_write = lock.write("foo_write").await;
        let _bar_write = lock.write("bar_write").await;
        let _foo_read = lock.read("foo_read").await;
        let _bar_read = lock.read("bar_read").await;
        assert_eq!(lock.locks.lock().await.len(), 4);
        drop(_foo_read);
        drop(_bar_write);
        lock.clean().await;
        assert_eq!(lock.locks.lock().await.len(), 2);
    }
}
