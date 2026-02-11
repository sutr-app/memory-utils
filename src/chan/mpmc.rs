use anyhow::{Result, anyhow};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::sync::Mutex;

use super::{ChanBuffer, ChanBufferItem, ChanTrait};

#[derive(Clone, Debug)]
pub struct Chan<T: Send + Sync + Clone> {
    pub tx: flume::Sender<T>,
    pub rx: flume::Receiver<T>,
    pub key_set: Arc<Mutex<HashSet<String>>>, // prevent duplicate key
}
impl<T: Send + Sync + Clone> Chan<T> {
    pub fn new(capacity: Option<usize>) -> Self {
        let (tx, rx) = capacity.map(flume::bounded).unwrap_or(flume::unbounded());
        Self {
            tx,
            rx,
            key_set: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn count(&self) -> usize {
        // maybe always tx.len() == rx.len() (shared queue behind)
        if self.tx.len() > self.rx.len() {
            self.tx.len()
        } else {
            self.rx.len()
        }
    }
    pub fn sender_count(&self) -> usize {
        self.tx.sender_count()
    }
    pub fn receiver_count(&self) -> usize
    where
        Self: Send + Sync,
    {
        self.rx.receiver_count()
    }
}
impl<T: Send + Sync + Clone + std::fmt::Debug + 'static> ChanTrait<T> for Chan<T> {
    fn new(channel_capacity: Option<usize>) -> Self {
        Chan::new(channel_capacity)
    }

    async fn send_to_chan(&self, data: T) -> Result<bool> {
        self.tx
            .send_async(data)
            .await
            .map_err(|e| anyhow!("send_to_chan error: {:?}", e))
            .map(|_| true)
    }

    async fn receive_from_chan(&self, recv_timeout: Option<Duration>) -> Result<T> {
        if let Some(dur) = recv_timeout {
            tokio::time::timeout(dur, self.rx.recv_async())
                .await
                .map_err(|e| anyhow!("chan recv timeout error: timeout={:?}, err={:?}", dur, e))?
                .map_err(|e| anyhow!("chan recv error: {:?}", e))
        } else {
            self.rx
                .recv_async()
                .await
                .map_err(|e| anyhow!("chan recv error: {:?}", e))
        }
    }
    async fn receive_stream_from_chan(
        &self,
        recv_timeout: Option<Duration>,
    ) -> Result<futures::stream::BoxStream<'static, T>>
    where
        T: 'static,
    {
        let rx = self.rx.clone();
        let stream = futures::stream::unfold(rx, move |rx| async move {
            if let Some(dur) = recv_timeout {
                match tokio::time::timeout(dur, rx.recv_async()).await {
                    Ok(Ok(item)) => Some((item, rx)),
                    _ => None,
                }
            } else {
                match rx.recv_async().await {
                    Ok(item) => Some((item, rx)),
                    Err(_) => None,
                }
            }
        });
        Ok(Box::pin(stream))
    }

    async fn try_receive_from_chan(&self) -> Result<T> {
        self.rx
            .try_recv()
            .map_err(|e| anyhow!("chan recv error: {:?}", e))
    }

    async fn receive_from_chan_with_check<F, Fut>(
        &self,
        recv_timeout: Option<Duration>,
        check: F,
    ) -> Result<T>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = Option<T>> + Send,
    {
        if let Some(value) = check().await {
            return Ok(value);
        }
        self.receive_from_chan(recv_timeout).await
    }

    fn key_set(&self) -> Arc<Mutex<HashSet<String>>> {
        self.key_set.clone()
    }

    fn count(&self) -> usize {
        self.count()
    }

    fn receiver_count(&self) -> usize {
        self.receiver_count()
    }
}

pub trait UseChanBuffer {
    type Item: Send + Sync + Clone + std::fmt::Debug;

    fn chan_buf(&self) -> &ChanBuffer<Self::Item, Chan<ChanBufferItem<Self::Item>>>;
}

#[cfg(test)]
mod tests {
    use crate::chan::ChanBuffer;

    use super::*;
    use tokio::time::{Duration, sleep};

    #[derive(Clone)]
    struct Test {
        pub chan_buf: ChanBuffer<Vec<u8>, Chan<ChanBufferItem<Vec<u8>>>>,
    }

    impl UseChanBuffer for Test {
        type Item = Vec<u8>;
        fn chan_buf(&self) -> &ChanBuffer<Vec<u8>, Chan<ChanBufferItem<Vec<u8>>>> {
            &self.chan_buf
        }
    }

    #[tokio::test]
    async fn test_use_chan_buf() {
        let test = Test {
            chan_buf: ChanBuffer::new(None, 10000),
        };
        let key = "test_key";
        let data = b"test".to_vec();
        assert_eq!(test.chan_buf().count_chan_opt(key).await, None);
        test.chan_buf()
            .send_to_chan(key, data.clone(), None, None, false)
            .await
            .unwrap();
        assert_eq!(test.chan_buf().count_chan_opt(key).await, Some(1));
        let recv_data = test
            .chan_buf()
            .receive_from_chan(key, None, None)
            .await
            .unwrap();
        assert_eq!(data, recv_data);
        assert_eq!(test.chan_buf().count_chan_opt(key).await, Some(0));
    }

    #[tokio::test]
    async fn test_duplicate_key() {
        let test = Test {
            chan_buf: ChanBuffer::new(None, 10000),
        };
        let key = "test";
        let data = b"test".to_vec();
        test.chan_buf()
            .send_to_chan(key, data.clone(), Some("test".to_string()), None, false)
            .await
            .unwrap();
        let res = test
            .chan_buf()
            .send_to_chan(key, data.clone(), Some("test".to_string()), None, false)
            .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_chan_buf_multi_thread() {
        let test = Test {
            chan_buf: ChanBuffer::new(None, 10000),
        };
        let test_clone = test.clone();
        let key = "test_key";
        let data = b"test".to_vec();
        let key_clone = key.to_string();
        let data_clone = data.clone();
        let handle = tokio::spawn(async move {
            let recv_data = test_clone
                .chan_buf()
                .receive_from_chan(&key_clone, None, None)
                .await
                .unwrap();
            assert_eq!(data_clone, recv_data);
        });
        sleep(Duration::from_secs(1)).await;
        test.chan_buf()
            .send_to_chan(key, data.clone(), None, None, false)
            .await
            .unwrap();
        handle.await.unwrap();
    }
    #[tokio::test]
    async fn test_chan_buf_recv_timeout() {
        let test = Test {
            chan_buf: ChanBuffer::new(None, 10000),
        };
        let test_clone = test.clone();
        let key = "test_key";
        let data = b"test".to_vec();
        let handle = tokio::spawn(async move {
            let res = test_clone
                .chan_buf()
                .receive_from_chan(key, Some(Duration::from_secs(1)), None)
                .await;
            assert!(res.is_err());
        });
        sleep(Duration::from_secs(2)).await;
        test.chan_buf()
            .send_to_chan(key, data.clone(), None, None, false)
            .await
            .unwrap();
        handle.await.unwrap();
    }
}
