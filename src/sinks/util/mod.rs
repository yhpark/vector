pub mod batch;
pub mod buffer;
pub mod http;
pub mod retries;
pub mod tls;

use crate::buffers::Acker;
use futures::{
    future, stream::FuturesUnordered, Async, AsyncSink, Future, Poll, Sink, StartSend, Stream,
};
use std::collections::HashMap;
use std::hash::Hash;
use std::time::Duration;
use tower::Service;

pub use batch::{Batch, BatchSink};
pub use buffer::{Buffer, Compression, PartitionBuffer, PartitionInnerBuffer};
pub use buffer::metrics::MetricBuffer;
pub use buffer::partition::{Partition, PartitionedBatchSink};

pub trait SinkExt<T>
where
    Self: Sink<SinkItem = T> + Sized,
{
    fn stream_ack(self, acker: Acker) -> StreamAck<Self> {
        StreamAck::new(self, acker)
    }

    fn batched(self, batch: T, limit: usize) -> BatchSink<T, Self>
    where
        T: Batch,
    {
        BatchSink::new(self, batch, limit)
    }

    fn batched_with_min(self, batch: T, min: usize, delay: Duration) -> BatchSink<T, Self>
    where
        T: Batch,
    {
        BatchSink::new_min(self, batch, min, Some(delay))
    }

    fn batched_with_max(self, batch: T, max: usize, delay: Duration) -> BatchSink<T, Self>
    where
        T: Batch,
    {
        BatchSink::new_min_max(self, batch, max, 0, Some(delay))
    }

    fn partitioned_batched_with_min<K>(
        self,
        batch: T,
        min: usize,
        delay: Duration,
    ) -> PartitionedBatchSink<T, Self, K>
    where
        T: Batch,
        K: Eq + Hash + Clone + Send + 'static,
    {
        PartitionedBatchSink::with_linger(self, batch, min, min, delay)
    }
}

impl<T, S> SinkExt<T> for S where S: Sink<SinkItem = T> + Sized {}

pub struct StreamAck<T> {
    inner: T,
    acker: Acker,
    pending: usize,
}

impl<T: Sink> StreamAck<T> {
    pub fn new(inner: T, acker: Acker) -> Self {
        Self {
            inner,
            acker,
            pending: 0,
        }
    }
}

impl<T: Sink> Sink for StreamAck<T> {
    type SinkItem = T::SinkItem;
    type SinkError = T::SinkError;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let res = self.inner.start_send(item);

        if let Ok(AsyncSink::Ready) = res {
            self.pending += 1;

            if self.pending >= 10000 {
                self.poll_complete()?;
            }
        }

        res
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        let res = self.inner.poll_complete();

        if let Ok(Async::Ready(_)) = res {
            self.acker.ack(self.pending);
            self.pending = 0;
        }

        res
    }
}

pub type MetadataFuture<F, M> = future::Join<F, future::FutureResult<M, <F as Future>::Error>>;

pub struct BatchServiceSink<T, S: Service<T>, B: Batch<Output = T>> {
    service: S,
    in_flight: FuturesUnordered<MetadataFuture<S::Future, (usize, usize)>>,
    _phantom: std::marker::PhantomData<(T, B)>,

    acker: Acker,
    seq_head: usize,
    seq_tail: usize,
    pending_acks: HashMap<usize, usize>,
}

impl<T, S, B> BatchServiceSink<T, S, B>
where
    S: Service<T>,
    B: Batch<Output = T>,
{
    pub fn new(service: S, acker: Acker) -> Self {
        Self {
            service,
            in_flight: FuturesUnordered::new(),
            acker,
            _phantom: std::marker::PhantomData,
            seq_head: 0,
            seq_tail: 0,
            pending_acks: HashMap::new(),
        }
    }
}

type Error = Box<dyn std::error::Error + 'static + Send + Sync>;

impl<T, S, B> Sink for BatchServiceSink<T, S, B>
where
    S: Service<T>,
    S::Error: Into<Error>,
    S::Response: std::fmt::Debug,
    B: Batch<Output = T>,
{
    type SinkItem = B;
    type SinkError = ();

    fn start_send(&mut self, batch: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let mut tried_once = false;
        loop {
            match self.service.poll_ready() {
                Ok(Async::Ready(())) => {
                    let items_in_batch = batch.num_items();
                    let seqno = self.seq_head;
                    self.seq_head += 1;
                    self.in_flight.push(
                        self.service
                            .call(batch.finish())
                            .join(future::ok((seqno, items_in_batch))),
                    );
                    return Ok(AsyncSink::Ready);
                }

                Ok(Async::NotReady) => {
                    if tried_once {
                        return Ok(AsyncSink::NotReady(batch));
                    } else {
                        self.poll_complete()?;
                        tried_once = true;
                    }
                }

                // TODO: figure out if/how to handle this
                Err(e) => panic!("service must be discarded: {}", e.into()),
            }
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        loop {
            match self.in_flight.poll() {
                Ok(Async::NotReady) => return Ok(Async::NotReady),

                Ok(Async::Ready(None)) => return Ok(Async::Ready(())),

                Ok(Async::Ready(Some((response, (seqno, batch_size))))) => {
                    self.pending_acks.insert(seqno, batch_size);

                    let mut num_to_ack = 0;
                    while let Some(ack_size) = self.pending_acks.remove(&self.seq_tail) {
                        num_to_ack += ack_size;
                        self.seq_tail += 1
                    }
                    self.acker.ack(num_to_ack);

                    trace!(message = "request succeeded.", ?response);
                }

                Err(error) => {
                    let error = error.into();
                    error!(
                        message = "request failed.",
                        error = tracing::field::display(&error)
                    )
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::BatchServiceSink;
    use crate::buffers::Acker;
    use crate::test_util::wait_for;
    use futures::{stream, sync::oneshot, Future, Poll, Sink};
    use std::sync::{atomic::Ordering, Arc, Mutex};
    use tokio::runtime::Runtime;
    use tower::Service;

    struct FakeService {
        senders: Arc<Mutex<Vec<oneshot::Sender<()>>>>,
    }

    impl FakeService {
        fn new() -> (Self, Arc<Mutex<Vec<oneshot::Sender<()>>>>) {
            let senders = Arc::new(Mutex::new(vec![]));

            let res = Self {
                senders: senders.clone(),
            };

            (res, senders)
        }
    }

    impl Service<Vec<()>> for FakeService {
        type Response = ();
        type Error = oneshot::Canceled;
        type Future = oneshot::Receiver<()>;

        fn poll_ready(&mut self) -> Poll<(), Self::Error> {
            Ok(().into())
        }

        fn call(&mut self, _items: Vec<()>) -> Self::Future {
            let (tx, rx) = oneshot::channel();
            self.senders.lock().unwrap().push(tx);

            rx
        }
    }

    #[test]
    fn batch_service_sink_acking() {
        let mut rt = Runtime::new().unwrap();

        let (service, senders) = FakeService::new();
        let (acker, ack_counter) = Acker::new_for_testing();

        let service_sink = BatchServiceSink::new(service, acker);

        let b1 = vec![(); 1];
        let b2 = vec![(); 2];
        let b3 = vec![(); 4];
        let b4 = vec![(); 8];
        let b5 = vec![(); 16];
        let b6 = vec![(); 32];

        rt.spawn(
            service_sink
                .send_all(stream::iter_ok(vec![b1, b2, b3, b4, b5, b6]))
                .map(|_| ()),
        );

        wait_for(|| senders.lock().unwrap().len() == 6);
        assert_eq!(0, ack_counter.load(Ordering::Relaxed));

        senders.lock().unwrap().remove(0).send(()).unwrap(); // 1
        wait_for(|| {
            let current = ack_counter.load(Ordering::Relaxed);
            assert!(current == 0 || current == 1);
            1 == current
        });

        senders.lock().unwrap().remove(1).send(()).unwrap(); // 4
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(1, ack_counter.load(Ordering::Relaxed));

        senders.lock().unwrap().remove(0).send(()).unwrap(); // 2
        wait_for(|| {
            let current = ack_counter.load(Ordering::Relaxed);
            assert!(current == 1 || current == 7);
            7 == current
        });

        senders.lock().unwrap().remove(0).send(()).unwrap(); // 8
        wait_for(|| {
            let current = ack_counter.load(Ordering::Relaxed);
            assert!(current == 7 || current == 15);
            15 == current
        });

        senders.lock().unwrap().remove(1).send(()).unwrap(); // 32
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(15, ack_counter.load(Ordering::Relaxed));

        drop(senders.lock().unwrap().remove(0)); // 16
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(15, ack_counter.load(Ordering::Relaxed));
    }
}
