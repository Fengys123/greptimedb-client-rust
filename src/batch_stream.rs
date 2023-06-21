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

    batch_opt: BatchOption,
}

pub struct BatchOption {
    pub delay: Option<Duration>,
    pub batch_size: Option<u32>,
}

impl<T> BatchStream<T> {
    pub fn new(stream: T, batch_opt: BatchOption) -> Self {
        Self {
            inner: stream,
            batch_opt,
        }
    }
}

impl<T> Stream for BatchStream<T>
where
    T: Stream<Item = GreptimeRequest>,
{
    type Item = GreptimeRequest;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        this.inner.poll_next(cx)
    }
}
