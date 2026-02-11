pub mod broadcast;
pub mod mpmc;

use super::cache::stretto::{MemoryCacheConfig, MemoryCacheImpl, UseMemoryCache};
use anyhow::{Result, anyhow};
use debug_stub_derive::DebugStub;
use futures::{
    Stream,
    stream::{BoxStream, StreamExt},
};
use std::{collections::HashSet, marker::PhantomData, sync::Arc, time::Duration};
use tokio::sync::Mutex;

pub trait ChanTrait<T: Send + Sync + Clone>: Send + Sync + std::fmt::Debug {
    fn new(channel_capacity: Option<usize>) -> Self;
    /// send data to channel
    /// # Arguments
    /// * `data` - data to send
    fn send_to_chan(&self, data: T) -> impl std::future::Future<Output = Result<bool>> + Send;
    /// receive data from channel
    /// # Arguments
    /// * `recv_timeout` - timeout for receive
    fn receive_from_chan(
        &self,
        recv_timeout: Option<Duration>,
    ) -> impl std::future::Future<Output = Result<T>> + Send;

    fn receive_stream_from_chan(
        &self,
        recv_timeout: Option<Duration>,
    ) -> impl std::future::Future<Output = Result<BoxStream<'static, T>>> + Send;

    fn try_receive_from_chan(&self) -> impl std::future::Future<Output = Result<T>> + Send;

    /// Like receive_from_chan, but calls an async check after subscriber registration
    /// and before blocking on recv. If check returns Some(value), returns it immediately.
    fn receive_from_chan_with_check<F, Fut>(
        &self,
        recv_timeout: Option<Duration>,
        check: F,
    ) -> impl std::future::Future<Output = Result<T>> + Send
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = Option<T>> + Send;

    fn key_set(&self) -> Arc<Mutex<HashSet<String>>>; // prevent duplicate key
    fn count(&self) -> usize;
    /// Returns the number of active receivers (subscribers)
    fn receiver_count(&self) -> usize;
}

pub type ChanBufferItem<T> = (Option<String>, T);

#[derive(Clone, DebugStub)]
pub struct ChanBuffer<T: Send + Sync + Clone + 'static, C: ChanTrait<ChanBufferItem<T>> + 'static> {
    channel_capacity: Option<usize>,
    chan_buf: MemoryCacheImpl<String, Arc<C>>,
    _phantom: PhantomData<T>,
}

impl<T: Send + Sync + Clone, C: ChanTrait<ChanBufferItem<T>>> ChanBuffer<T, C> {
    pub fn new(channel_capacity: Option<usize>, max_channels: usize) -> Self {
        Self {
            channel_capacity,
            chan_buf: super::cache::stretto::MemoryCacheImpl::new(
                &MemoryCacheConfig {
                    num_counters: max_channels,
                    max_cost: max_channels as i64,
                    use_metrics: false,
                },
                None,
            ),
            _phantom: PhantomData,
        }
    }

    pub async fn get_chan_if_exists(&self, name: impl Into<String>) -> Option<Arc<C>> {
        self.chan_buf.find_cache_locked(&name.into()).await
    }

    async fn get_or_create_chan(
        &self,
        name: impl Into<String>,
        ttl: Option<&Duration>,
    ) -> Result<Arc<C>> {
        let k = name.into();
        self.chan_buf
            .with_cache_locked(&k, ttl, || async {
                tracing::debug!("create new channel: {}", &k);
                let ch = Arc::new(C::new(self.channel_capacity));
                Ok(ch)
            })
            .await
    }

    pub async fn clear_chan_all(&self) -> Result<()> {
        self.chan_buf.clear().await
    }

    /// Delete a specific channel by name
    /// Returns true if the channel existed and was deleted
    pub async fn delete_chan(&self, name: impl Into<String>) -> Result<bool> {
        let k = name.into();
        match self.chan_buf.delete_cache_locked(&k).await {
            Ok(()) => {
                tracing::debug!("deleted channel: {}", &k);
                Ok(true)
            }
            Err(e) => {
                tracing::warn!("failed to delete channel {}: {:?}", &k, e);
                Err(e)
            }
        }
    }

    /// Returns the number of active receivers for the specified channel
    /// Returns 0 if the channel doesn't exist
    pub async fn receiver_count(&self, name: impl Into<String>) -> usize {
        self.get_chan_if_exists(name)
            .await
            .map(|ch| ch.receiver_count())
            .unwrap_or(0)
    }

    /// Send data to a named channel.
    ///
    /// Returns `Ok(true)` if the data was sent to at least one receiver,
    /// `Ok(false)` if the channel has no receivers or (when `only_if_exists`
    /// is true) the channel does not exist yet.
    /// Returns `Err` only on real failures (e.g., serialization error,
    /// duplicate `uniq_key`).
    pub async fn send_to_chan(
        &self,
        name: impl Into<String> + Send,
        data: T,
        uniq_key: Option<String>,
        ttl: Option<&Duration>,
        only_if_exists: bool,
    ) -> Result<bool> {
        let nm = name.into();
        let chan = if only_if_exists {
            match self.get_chan_if_exists(nm.clone()).await {
                Some(ch) => ch,
                None => {
                    tracing::debug!("channel not found (no subscriber): {}", nm);
                    return Ok(false);
                }
            }
        } else {
            self.get_or_create_chan(nm.clone(), ttl).await?
        };
        if let Some(ukey) = &uniq_key {
            let key_set = chan.key_set();
            let mut ksl = key_set.lock().await;
            if !ksl.insert(ukey.clone()) {
                tracing::warn!("duplicate key: {}", ukey);
                return Err(anyhow!("duplicate uniq_key: {}", ukey));
            }
        }
        chan.send_to_chan((uniq_key, data)).await
    }
    /// Send a stream of data items to a named channel.
    ///
    /// Returns `Ok(true)` on success, `Ok(false)` if the channel does not
    /// exist when `only_if_exists` is true.
    /// Returns `Err` only on real failures.
    pub async fn send_stream_to_chan(
        &self,
        name: impl Into<String> + Send,
        stream: impl Stream<Item = T> + Send + 'static,
        uniq_key: Option<String>,
        ttl: Option<&Duration>,
        only_if_exists: bool,
    ) -> Result<bool> {
        let nm = name.into();
        let chan = if only_if_exists {
            match self.get_chan_if_exists(nm.clone()).await {
                Some(ch) => ch,
                None => {
                    tracing::debug!("channel not found (no subscriber): {}", nm);
                    return Ok(false);
                }
            }
        } else {
            self.get_or_create_chan(nm.clone(), ttl).await?
        };

        if let Some(ukey) = &uniq_key {
            let key_set = chan.key_set();
            let mut ksl = key_set.lock().await;
            if !ksl.insert(ukey.clone()) {
                tracing::warn!("duplicate key: {}", ukey);
                return Err(anyhow!("duplicate uniq_key: {}", ukey));
            }
        }
        let nm_clone = nm.clone();
        // send stream to channel with iteration
        let uniq_key_clone = uniq_key.clone();
        stream
            .map(move |data| Ok((uniq_key_clone.clone(), data)))
            // .for_each_concurrent(None, |data_result| async {
            .for_each(
                move |data_result: Result<(Option<String>, T), anyhow::Error>| {
                    let nm_clone = nm.clone();
                    let c_clone = chan.clone();
                    async move {
                        if let Ok(data) = data_result {
                            match c_clone.send_to_chan(data).await {
                                Err(e) => {
                                    tracing::error!(
                                        "send data error on channel '{}': {:?}",
                                        &nm_clone,
                                        e
                                    );
                                }
                                _ => {
                                    tracing::debug!("===== send data to channel: {}", &nm_clone);
                                }
                            }
                        }
                    }
                },
            )
            .await;
        tracing::debug!("=== sent stream to channel: {}", &nm_clone);
        Ok(true)
    }
    /// receive data from channel
    /// # Arguments
    /// * `name` - channel name
    /// * `recv_timeout` - timeout for receive
    /// * `ttl` - ttl for channel (cannot change ttl of each named channel after created)
    pub async fn receive_from_chan(
        &self,
        name: impl Into<String> + Send,
        recv_timeout: Option<Duration>,
        ttl: Option<&Duration>,
    ) -> Result<T> {
        let nm = name.into();
        let chan = self.get_or_create_chan(nm.clone(), ttl).await?;
        let (res, key_set) = if let Some(dur) = recv_timeout {
            let key_set = chan.key_set();
            // XXX unique key may not be removed
            (
                tokio::time::timeout(dur, chan.receive_from_chan(recv_timeout))
                    .await
                    .map_err(|e| {
                        anyhow!("chan recv timeout error: timeout={:?}, err={:?}", dur, e)
                    })?
                    .map_err(|e| anyhow!("chan recv error: {:?}", e))?,
                key_set,
            )
        } else {
            let key_set = chan.key_set();
            (
                chan.receive_from_chan(None)
                    .await
                    .map_err(|e| anyhow!("chan recv error: {:?}", e))?,
                key_set,
            )
        };
        if let Some(ukey) = res.0 {
            let mut ksl = key_set.lock().await;
            ksl.remove(&ukey);
        }
        Ok(res.1)
    }

    /// Like receive_from_chan, but calls an async check after subscriber registration
    /// and before blocking on recv. If check returns Some(value), returns it immediately.
    pub async fn receive_from_chan_with_check<F, Fut>(
        &self,
        name: impl Into<String> + Send,
        recv_timeout: Option<Duration>,
        ttl: Option<&Duration>,
        check: F,
    ) -> Result<T>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = Option<ChanBufferItem<T>>> + Send,
    {
        let nm = name.into();
        let chan = self.get_or_create_chan(nm.clone(), ttl).await?;
        let (res, key_set) = if let Some(dur) = recv_timeout {
            let key_set = chan.key_set();
            (
                tokio::time::timeout(dur, chan.receive_from_chan_with_check(None, check))
                    .await
                    .map_err(|e| {
                        anyhow!("chan recv timeout error: timeout={:?}, err={:?}", dur, e)
                    })?
                    .map_err(|e| anyhow!("chan recv error: {:?}", e))?,
                key_set,
            )
        } else {
            let key_set = chan.key_set();
            (
                chan.receive_from_chan_with_check(None, check)
                    .await
                    .map_err(|e| anyhow!("chan recv error: {:?}", e))?,
                key_set,
            )
        };
        if let Some(ukey) = res.0 {
            let mut ksl = key_set.lock().await;
            ksl.remove(&ukey);
        }
        Ok(res.1)
    }

    pub async fn receive_stream_from_chan<N: Into<String> + Send>(
        &self,
        name: N,
        ttl: Option<Duration>,
    ) -> Result<impl Stream<Item = T> + Send + use<T, C, N>> {
        let nm = name.into();
        tracing::debug!("receive stream from chan: {}", &nm);
        let chan = self.get_or_create_chan(nm.clone(), ttl.as_ref()).await?;

        tracing::debug!("receive stream from chan: try unfold stream {}", &nm);
        let nm_clone = nm.clone();
        match chan.receive_stream_from_chan(ttl).await {
            Ok(st) => {
                tracing::debug!("receive stream data from chan: {}", &nm_clone);
                Ok(st.map(|(_opt_key, data)| data))
            }
            Err(e) => {
                tracing::error!("receive stream data from chan error: {:?}", e);
                Err(e)
            }
        }
    }

    pub async fn try_receive_from_chan(
        &self,
        name: impl Into<String> + Send,
        ttl: Option<&Duration>,
    ) -> Result<T> {
        let nm = name.into();
        let res: (Option<String>, T) = self
            .get_or_create_chan(nm.clone(), ttl)
            .await?
            .try_receive_from_chan()
            .await?;
        if let Some(ukey) = res.0 {
            let chan = self.get_or_create_chan(nm, ttl).await?;
            let key_set = chan.key_set();
            let mut ksl = key_set.lock().await;
            ksl.remove(&ukey);
        }
        Ok(res.1)
    }
    pub async fn count_chan_opt(&self, name: impl Into<String>) -> Option<usize> {
        self.get_chan_if_exists(name).await.map(|ch| ch.count())
    }
}

// // create test for UseChanPool
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use tokio::time::{sleep, Duration};

//     #[derive(Clone)]
//     struct Test {
//         pub chan_pool: ChanBuffer,
//     }

//     impl UseChanBuffer for Test {
//         fn chan_buf(&self) -> &ChanBuffer {
//             &self.chan_pool
//         }
//     }

//     #[tokio::test]
//     async fn test_use_chan_pool() {
//         let test = Test {
//             chan_pool: ChanBuffer::new(None, 10000),
//         };
//         let key = "test_key";
//         let data = b"test".to_vec();
//         assert_eq!(test.count_chan(key).await, 0);
//         test.send_to_chan(key, data.clone(), None, None)
//             .await
//             .unwrap();
//         assert_eq!(test.count_chan(key).await, 1);
//         let recv_data = test.receive_from_chan(key, None, None).await.unwrap();
//         assert_eq!(data, recv_data);
//         assert_eq!(test.count_chan(key).await, 0);
//     }

//     #[tokio::test]
//     async fn test_duplicate_key() {
//         let test = Test {
//             chan_pool: ChanBuffer::new(None, 10000),
//         };
//         let key = "test";
//         let data = b"test".to_vec();
//         test.send_to_chan(key, data.clone(), Some("test".to_string()), None)
//             .await
//             .unwrap();
//         let res = test
//             .send_to_chan(key, data.clone(), Some("test".to_string()), None)
//             .await;
//         assert!(res.is_err());
//     }

//     #[tokio::test]
//     async fn test_chan_pool_multi_thread() {
//         let test = Test {
//             chan_pool: ChanBuffer::new(None, 10000),
//         };
//         let test_clone = test.clone();
//         let key = "test_key";
//         let data = b"test".to_vec();
//         let key_clone = key.to_string();
//         let data_clone = data.clone();
//         let handle = tokio::spawn(async move {
//             let recv_data = test_clone
//                 .receive_from_chan(&key_clone, None, None)
//                 .await
//                 .unwrap();
//             assert_eq!(data_clone, recv_data);
//         });
//         sleep(Duration::from_secs(1)).await;
//         test.send_to_chan(key, data.clone(), None, None)
//             .await
//             .unwrap();
//         handle.await.unwrap();
//     }
//     #[tokio::test]
//     async fn test_chan_pool_recv_timeout() {
//         let test = Test {
//             chan_pool: ChanBuffer::new(None, 10000),
//         };
//         let test_clone = test.clone();
//         let key = "test_key";
//         let data = b"test".to_vec();
//         let handle = tokio::spawn(async move {
//             let res = test_clone
//                 .receive_from_chan(key, Some(Duration::from_secs(1)), None)
//                 .await;
//             assert!(res.is_err());
//         });
//         sleep(Duration::from_secs(2)).await;
//         test.send_to_chan(key, data.clone(), None, None)
//             .await
//             .unwrap();
//         handle.await.unwrap();
//     }
// }
#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::sync::broadcast;
    use tokio::sync::oneshot;
    use tokio_stream::wrappers::BroadcastStream;

    #[derive(Debug)]
    struct DummyChan<T: Send + Sync + Clone + 'static> {
        sender: broadcast::Sender<ChanBufferItem<T>>,
        key_set: Arc<Mutex<HashSet<String>>>,
    }

    impl<T: Send + Sync + Clone + 'static> Clone for DummyChan<T> {
        fn clone(&self) -> Self {
            DummyChan {
                sender: self.sender.clone(),
                key_set: self.key_set.clone(),
            }
        }
    }

    impl<T: Send + Sync + Clone + std::fmt::Debug + 'static> ChanTrait<ChanBufferItem<T>>
        for DummyChan<T>
    {
        fn new(channel_capacity: Option<usize>) -> Self {
            let capacity = channel_capacity.unwrap_or(10);
            let (sender, _) = broadcast::channel(capacity);
            DummyChan {
                sender,
                key_set: Arc::new(Mutex::new(HashSet::new())),
            }
        }

        fn send_to_chan(
            &self,
            data: ChanBufferItem<T>,
        ) -> impl std::future::Future<Output = Result<bool>> + Send {
            let sender = self.sender.clone();
            async move {
                sender
                    .send(data)
                    .map(|_| true)
                    .map_err(|e| anyhow!("send_to_chan error: {:?}", e))
            }
        }

        fn receive_from_chan(
            &self,
            _recv_timeout: Option<Duration>,
        ) -> impl std::future::Future<Output = Result<ChanBufferItem<T>>> + Send {
            let mut receiver = self.sender.subscribe();
            async move {
                receiver
                    .recv()
                    .await
                    .map_err(|e| anyhow!("receive_from_chan error: {:?}", e))
            }
        }

        fn receive_stream_from_chan(
            &self,
            _recv_timeout: Option<Duration>,
        ) -> impl std::future::Future<
            Output = Result<futures::stream::BoxStream<'static, ChanBufferItem<T>>>,
        > + Send {
            let receiver = self.sender.subscribe();
            async move {
                let stream = BroadcastStream::new(receiver).filter_map(|res| async { res.ok() });
                Ok(Box::pin(stream) as futures::stream::BoxStream<'static, ChanBufferItem<T>>)
            }
        }

        fn try_receive_from_chan(
            &self,
        ) -> impl std::future::Future<Output = Result<ChanBufferItem<T>>> + Send {
            let mut receiver = self.sender.subscribe();
            async move {
                receiver
                    .try_recv()
                    .map_err(|e| anyhow!("try_receive_from_chan error: {:?}", e))
            }
        }

        fn receive_from_chan_with_check<F, Fut>(
            &self,
            _recv_timeout: Option<Duration>,
            check: F,
        ) -> impl std::future::Future<Output = Result<ChanBufferItem<T>>> + Send
        where
            F: FnOnce() -> Fut + Send,
            Fut: std::future::Future<Output = Option<ChanBufferItem<T>>> + Send,
        {
            let mut receiver = self.sender.subscribe();
            async move {
                if let Some(value) = check().await {
                    return Ok(value);
                }
                receiver
                    .recv()
                    .await
                    .map_err(|e| anyhow!("receive_from_chan_with_check error: {:?}", e))
            }
        }

        fn key_set(&self) -> Arc<Mutex<HashSet<String>>> {
            self.key_set.clone()
        }

        fn count(&self) -> usize {
            0
        }

        fn receiver_count(&self) -> usize {
            self.sender.receiver_count()
        }
    }

    // Helper structure using ChanBuffer with DummyChan.
    #[derive(Clone)]
    struct TestChanBuffer {
        chan_buf: ChanBuffer<Vec<u8>, DummyChan<Vec<u8>>>,
    }

    impl TestChanBuffer {
        fn new() -> Self {
            Self {
                chan_buf: ChanBuffer::new(Some(10), 10000),
            }
        }

        async fn send(
            &self,
            key: &str,
            data: Vec<u8>,
            uniq_key: Option<String>,
            ttl: Option<&Duration>,
        ) -> Result<bool> {
            self.chan_buf
                .send_to_chan(key, data, uniq_key, ttl, false)
                .await
        }

        async fn receive(
            &self,
            key: &str,
            timeout: Option<Duration>,
            ttl: Option<&Duration>,
        ) -> Result<Vec<u8>> {
            self.chan_buf.receive_from_chan(key, timeout, ttl).await
            // .inspect(|data| {
            //     println!("===== received data: {:?}", data);
            // })
        }

        // async fn try_receive(&self, key: &str, ttl: Option<&Duration>) -> Result<Vec<u8>> {
        //     self.chan_buf.try_receive_from_chan(key, ttl).await
        // }

        async fn send_stream(
            &self,
            key: &str,
            data: Vec<Vec<u8>>,
            uniq_key: Option<String>,
            ttl: Option<&Duration>,
        ) -> Result<bool> {
            let stream = futures::stream::iter(data.into_iter()).then(|item| async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                // println!("===== send stream item: {:?}", item);
                item
            });
            self.chan_buf
                .send_stream_to_chan(key, stream, uniq_key, ttl, true)
                .await
        }
    }

    #[tokio::test]
    async fn test_send_and_receive() {
        let test_buf = TestChanBuffer::new();
        let key = "channel1";
        let data = b"hello".to_vec();

        // oneshotで受信側のサブスクライブ準備を待つ
        let (ready_tx, ready_rx) = oneshot::channel();
        let recv_task = tokio::spawn({
            let test_buf = test_buf.clone();
            let key = key.to_string();
            async move {
                let _ = ready_tx.send(());
                test_buf
                    .receive(&key, Some(Duration::from_secs(5)), None)
                    .await
            }
        });
        let _ = ready_rx.await;
        let send_result = test_buf.send(key, data.clone(), None, None).await;
        assert!(send_result.is_ok());
        let recv_result = recv_task.await.unwrap();
        assert!(recv_result.is_ok());
        assert_eq!(recv_result.unwrap(), data);
    }

    #[tokio::test]
    async fn test_send_stream() {
        let test_buf = TestChanBuffer::new();
        let key = "channel_stream";
        let data_items = vec![
            b"stream1".to_vec(),
            b"stream2".to_vec(),
            b"stream3".to_vec(),
            b"stream4".to_vec(),
            b"stream5".to_vec(),
        ];
        // ignored send result
        let _ = test_buf
            .send_stream(key, data_items.clone(), None, None)
            .await;

        let data_items_clone = data_items.clone();
        let recv_task = tokio::spawn({
            let test_buf = test_buf.clone();
            let key = key.to_string();
            async move {
                let mut results = vec![];
                for _ in 0..data_items_clone.len() {
                    let (ready_tx, ready_rx) = oneshot::channel();
                    let r = async {
                        let test_buf = test_buf.clone();
                        let key = key.clone();
                        let _ = ready_tx.send(());
                        test_buf
                            .receive(&key, Some(Duration::from_secs(10)), None)
                            .await
                    };
                    let res = r.await;
                    let _ = ready_rx.await;
                    results.push(res?);
                }
                Ok(results) as Result<Vec<Vec<u8>>>
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        let send_result = test_buf
            .send_stream(key, data_items.clone(), None, None)
            .await;
        // println!("===== send stream result: {:?}", send_result);
        assert!(send_result.is_ok());

        let recv_results = recv_task.await.unwrap().unwrap();
        assert_eq!(recv_results, data_items);
    }

    #[tokio::test]
    async fn test_duplicate_key() {
        let test_buf = TestChanBuffer::new();
        let key = "channel_dup";
        let data = b"data".to_vec();
        let uniq = Some("unique".to_string());
        // receiver
        tokio::spawn({
            let test_buf = test_buf.clone();
            let key = key.to_string();
            async move {
                let _ = test_buf.receive(&key, None, None).await;
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let send1 = test_buf.send(key, data.clone(), uniq.clone(), None).await;
        assert!(send1.is_ok());
        let send2 = test_buf.send(key, data, uniq, None).await;
        assert!(send2.is_err());
        // not send error but uniq_key error
        assert!(
            send2
                .err()
                .unwrap()
                .to_string()
                .contains("duplicate uniq_key")
        );
    }

    // #[tokio::test]
    // async fn test_try_receive() {
    //     let test_buf = TestChanBuffer::new();
    //     let key = "channel_try";
    //     let data = b"try_data".to_vec();

    //     let _ = test_buf.send(key, data.clone(), None, None).await;
    //     let (ready_tx, ready_rx) = oneshot::channel();
    //     let recv_task = tokio::spawn({
    //         let test_buf = test_buf.clone();
    //         let key = key.to_string();
    //         async move {
    //             let _ = ready_tx.send(());
    //             test_buf.try_receive(&key, None).await
    //         }
    //     });
    //     let _ = ready_rx.await;
    //     let try_recv = recv_task.await.unwrap();
    //     println!("===== try_recv: {:?}", try_recv);
    //     assert!(try_recv.is_ok());
    //     assert_eq!(try_recv.unwrap(), data);

    //     // empty error
    //     let try_recv_empty = test_buf.try_receive(key, None).await;
    //     assert!(try_recv_empty.is_err());
    // }

    #[tokio::test]
    async fn test_receive_timeout() {
        let test_buf = TestChanBuffer::new();
        let key = "channel_timeout";

        let (ready_tx, ready_rx) = oneshot::channel();
        let recv_task = tokio::spawn({
            let test_buf = test_buf.clone();
            let key = key.to_string();
            async move {
                let _ = ready_tx.send(());
                test_buf
                    .receive(&key, Some(Duration::from_millis(100)), None)
                    .await
            }
        });
        let _ = ready_rx.await;
        // timeout
        let recv_result = recv_task.await.unwrap();
        assert!(recv_result.is_err());
        assert!(
            recv_result
                .err()
                .unwrap()
                .to_string()
                .contains("chan recv timeout error")
        );
    }

    #[tokio::test]
    async fn test_async_send_after_recv_ready() {
        use tokio::sync::oneshot;
        let test_buf = TestChanBuffer::new();
        let key = "async_channel";
        let data = b"async_hello".to_vec();

        let (ready_tx, ready_rx) = oneshot::channel();

        let recv_task = tokio::spawn({
            let test_buf = test_buf.clone();
            let key = key.to_string();
            async move {
                let _ = ready_tx.send(());
                test_buf
                    .receive(&key, Some(Duration::from_secs(5)), None)
                    .await
            }
        });

        let _ = ready_rx.await;

        let data_clone = data.clone();
        let send_task = tokio::spawn({
            let test_buf = test_buf.clone();
            let key = key.to_string();
            async move { test_buf.send(&key, data_clone, None, None).await }
        });

        let send_result = send_task.await.unwrap();
        assert!(send_result.is_ok());

        let recv_result = recv_task.await.unwrap();
        assert!(recv_result.is_ok());
        assert_eq!(recv_result.unwrap(), data);
    }
}
