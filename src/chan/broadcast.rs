use super::{ChanBuffer, ChanBufferItem, ChanTrait};
use anyhow::{anyhow, Result};
use futures::{stream::BoxStream, StreamExt as _};
use std::{collections::HashSet, sync::Arc};
use tokio::sync::{
    broadcast::{self, error::SendError},
    Mutex,
};
use tokio_stream::wrappers::BroadcastStream;

#[derive(Clone, Debug)]
pub struct BroadcastChan<T: Send + Sync + Clone + 'static> {
    pub sender: Arc<broadcast::Sender<T>>,
    pub key_set: Arc<Mutex<HashSet<String>>>,
}
impl<T: Send + Sync + Clone + 'static> BroadcastChan<T> {
    // 32MB
    const DEFAULT_BUF_SIZE: usize = 32_000_000;
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        let sref = Arc::new(sender);
        Self {
            sender: sref,
            key_set: Arc::new(Mutex::new(HashSet::new())),
        }
    }
    pub fn send(&self, data: T) -> Result<bool> {
        // XXX may panic if over capacity (not unwind safe)
        match self.sender.send(data) {
            Ok(_) => Ok(true),
            // no receivers
            Err(SendError(_)) => Ok(false),
        }
    }
    pub async fn receiver(&self) -> broadcast::Receiver<T> {
        self.sender.subscribe()
    }
    pub fn count(&self) -> usize {
        self.sender.len()
    }
}
impl<T: Send + Sync + Clone + 'static + std::fmt::Debug> ChanTrait<T> for BroadcastChan<T> {
    fn new(buf_size: Option<usize>) -> Self {
        Self::new(buf_size.unwrap_or_else(|| {
            let bytes = std::mem::size_of::<T>() as f64;
            ((Self::DEFAULT_BUF_SIZE as f64) / bytes) as usize
        }))
    }

    async fn send_to_chan(&self, data: T) -> Result<bool> {
        match self.send(data) {
            Ok(b) => Ok(b),
            Err(e) => Err(anyhow!("send_to_chan error: {:?}", e)),
        }
    }

    async fn receive_from_chan(&self, recv_timeout: Option<std::time::Duration>) -> Result<T>
    where
        Self: Send + Sync,
    {
        let mut receiver = self.receiver().await;
        match recv_timeout {
            Some(dur) => {
                tracing::debug!("receive_from_chan with timeout: {:?}", dur);
                tokio::time::timeout(dur, receiver.recv())
                    .await
                    .map_err(|e| anyhow!("chan recv timeout error: {:?}", e))?
                    .map_err(|e| anyhow!("chan recv error: {:?}", e))
            }
            None => {
                tracing::debug!("receive_from_chan without timeout");
                receiver
                    .recv()
                    .await
                    .map_err(|e| anyhow!("chan recv error: {:?}", e))
            }
        }
    }

    async fn receive_stream_from_chan(
        &self,
        recv_timeout: Option<std::time::Duration>,
    ) -> Result<BoxStream<'static, T>>
    where
        Self: Send + Sync,
    {
        let receiver = self.receiver().await;
        let stream = BroadcastStream::new(receiver);

        let stream: BoxStream<'static, T> = match recv_timeout {
            Some(dur) => {
                tracing::debug!(
                    "receive_stream_from_chan with timeout(each 1 item received): {:?}",
                    dur
                );
                let stream = tokio_stream::StreamExt::timeout(stream, dur);
                stream
                    .take_while(|i| futures::future::ready(i.is_ok())) // timeout
                    .filter_map(|result| async {
                        match result {
                            Ok(Ok(value)) => {
                                tracing::debug!("===== receive_stream_from_chan: {:?}", value);
                                Some(value)
                            }
                            Ok(Err(e)) => {
                                tracing::error!("chan recv error: {:?}", e);
                                None
                            }
                            Err(e) => {
                                tracing::warn!("chan recv timeout error: {:?}", e);
                                None
                            }
                        }
                    })
                    .boxed()
            }
            None => {
                tracing::debug!("receive_stream_from_chan without timeout");
                stream
                    .filter_map(|result| async {
                        match result {
                            Ok(value) => Some(value),
                            Err(e) => {
                                tracing::error!("chan recv error: {:?}", e);
                                None
                            }
                        }
                    })
                    .boxed()
            }
        };
        Ok(stream)
    }
    async fn try_receive_from_chan(&self) -> Result<T> {
        let mut receiver = self.receiver().await;
        receiver
            .try_recv()
            .map_err(|e| anyhow!("chan recv error: {:?}", e))
    }

    fn key_set(&self) -> Arc<tokio::sync::Mutex<std::collections::HashSet<String>>> {
        self.key_set.clone()
    }

    fn count(&self) -> usize {
        self.count()
    }
}
pub trait UseBroadcastChanBuffer {
    type Item: Send + Sync + Clone + std::fmt::Debug;
    fn broadcast_chan_buf(
        &self,
    ) -> &ChanBuffer<Self::Item, BroadcastChan<ChanBufferItem<Self::Item>>>;
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::{BroadcastChan, UseBroadcastChanBuffer};
    use crate::chan::{ChanBuffer, ChanBufferItem, ChanTrait};
    use anyhow::Result;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_broadcast_chan() -> Result<()> {
        let chan = BroadcastChan::new(5);
        let data = vec![1, 2, 3];
        // no receiver
        assert!(!chan.send(data.clone()).unwrap());

        // 10 receiver
        let mut jhv = Vec::with_capacity(10);
        for _i in 0..10 {
            let d = data.clone();
            let ch = chan.clone();
            let jh = tokio::spawn(async move {
                let mut receiver = ch.receiver().await;
                let received = receiver.recv().await.unwrap();
                assert_eq!(d, received);
            });
            jhv.push(jh);
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        assert!(chan.send(data.clone()).unwrap());
        let res = futures::future::join_all(jhv).await;
        assert!(res.iter().all(|r| r.is_ok()));
        Ok(())
    }

    #[derive(Clone)]
    struct Test {
        pub chan_pool: ChanBuffer<Vec<u8>, BroadcastChan<ChanBufferItem<Vec<u8>>>>,
    }

    impl UseBroadcastChanBuffer for Test {
        type Item = Vec<u8>;
        fn broadcast_chan_buf(
            &self,
        ) -> &ChanBuffer<Vec<u8>, BroadcastChan<ChanBufferItem<Vec<u8>>>> {
            &self.chan_pool
        }
    }
    #[tokio::test]
    async fn test_use_broadcast_chan_buf() -> Result<()> {
        let test = Test {
            chan_pool: ChanBuffer::new(None, 10000),
        };
        let key = "test_key";
        let data = b"test".to_vec();
        assert_eq!(test.broadcast_chan_buf().count_chan_opt(key).await, None);
        let r = test
            .broadcast_chan_buf()
            .send_to_chan(
                key,
                data.clone(),
                None,
                Some(&Duration::from_secs(3)),
                false,
            )
            .await
            .unwrap();
        // no receiver
        assert!(!r);
        assert_eq!(test.broadcast_chan_buf().count_chan_opt(key).await, Some(0));
        let mut jhv = Vec::with_capacity(10);
        for _i in 0..10 {
            let test1 = test.clone();
            let data1 = data.clone();
            let jh = tokio::spawn(async move {
                let recv_data = test1
                    .broadcast_chan_buf()
                    .receive_from_chan(key, Some(Duration::from_secs(1)), None)
                    .await
                    .unwrap();
                assert_eq!(data1, recv_data);
            });
            jhv.push(jh);
        }
        // wait for receiver
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        test.broadcast_chan_buf()
            .send_to_chan(key, data.clone(), None, None, false)
            .await
            .unwrap();
        // no receiver
        let res = futures::future::join_all(jhv).await;

        assert!(res.len() == 10);
        assert!(res.iter().all(|r| r.is_ok()));
        Ok(())
    }

    #[tokio::test]
    async fn test_receive_stream_from_chan_no_timeout() -> Result<()> {
        let chan = BroadcastChan::new(10);
        // Send a few messages into the channel.
        let msg_values = vec![100, 200, 300, 400, 500];
        // ignore sent value before receive
        chan.send(10)?;
        // Subscribe to the stream before sending messages.
        let mut stream = chan
            .receive_stream_from_chan(None)
            .await?
            .take_while(|i| futures::future::ready(*i < 500))
            .boxed();
        let chan_clone = chan.clone();
        let msg_values_clone = msg_values.clone();
        tokio::spawn(async move {
            for &val in &msg_values_clone {
                chan_clone.send(val).unwrap();
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
        let mut collected = Vec::new();
        while let Some(value) = stream.next().await {
            // println!("==== value: {:?}", value);
            collected.push(value);
        }

        // println!("==== collected: {:?}", collected);
        assert_eq!(
            collected,
            msg_values
                .into_iter()
                .filter(|&i| i < 500)
                .collect::<Vec<_>>()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_receive_stream_from_chan_with_timeout() -> Result<()> {
        let chan = BroadcastChan::new(10);
        // Send a few messages into the channel.
        let msg_values = vec![100, 200, 300, 400, 500];
        // ignore sent value before receive
        chan.send(10)?;
        // Subscribe to the stream before sending messages.
        let mut stream = chan
            .receive_stream_from_chan(Some(Duration::from_millis(350)))
            .await?
            .boxed();
        let chan_clone = chan.clone();
        let msg_values_clone = msg_values.clone();
        tokio::spawn(async move {
            for &val in &msg_values_clone {
                tokio::time::sleep(Duration::from_millis(val)).await;
                chan_clone.send(val).unwrap();
            }
        });
        let mut collected = Vec::new();
        while let Some(value) = stream.next().await {
            collected.push(value);
        }

        assert_eq!(
            collected,
            msg_values
                .into_iter()
                .filter(|&i| i < 350)
                .collect::<Vec<_>>()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_receive_stream_from_chan_with_timeout2() -> Result<()> {
        let chan = BroadcastChan::new(10);
        // Spawn a receiver to ensure send returns true.
        let _ = chan.receiver().await;

        // Spawn a sender that sends messages periodically.
        let sender_chan = chan.clone();
        tokio::spawn(async move {
            for i in 0..5 {
                // Wait for 200ms between messages.
                tokio::time::sleep(Duration::from_millis(200)).await;
                sender_chan.send(i).unwrap();
            }
        });

        // With a timeout shorter than the overall sending period but long enough for individual messages.
        let stream = chan
            .receive_stream_from_chan(Some(Duration::from_millis(500)))
            .await?;
        let collected: Vec<_> = stream.take(5).collect().await;

        // Even with a per-item timeout, we expect to receive all messages in order.
        assert_eq!(collected, vec![0, 1, 2, 3, 4]);
        Ok(())
    }
}
