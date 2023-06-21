use futures_util::Stream;
use greptime_proto::v1::GreptimeRequest;
use pin_project::pin_project;
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

#[pin_project]
pub struct BatchStream<T> {
    #[pin]
    inner: T,

    delay: Option<Duration>,

    batch_size: Option<u32>,
}

impl<T> BatchStream<T> {
    pub fn new(stream: T, delay: Option<Duration>, batch_size: Option<u32>) -> Self {
        Self {
            inner: stream,
            delay,
            batch_size,
        }
    }
}

impl<T> Stream for BatchStream<T>
where
    T: Stream<Item = GreptimeRequest>,
{
    type Item = Vec<GreptimeRequest>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let inner = this.inner;

        match inner.poll_next(cx) {
            Poll::Ready(req) => Poll::Ready(req.map(|req| vec![req])),
            Poll::Pending => Poll::Pending,
        }
    }
}
