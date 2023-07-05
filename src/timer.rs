use std::pin::Pin;
use std::time::Duration;
use std::{future::Future, task::Poll};

use tokio::{task::JoinHandle, time::sleep};

/// A non-blocking timer that starts running in the background. Implements
/// `Future` so you can `.await` to manually wait for the time to run out.
pub struct Timer(JoinHandle<()>);

impl Timer {
    /// Creates a new `Timer` that starts running in the background. You can
    /// `.await` it to manually synchronize an action with the timer finishing.
    /// After `millis`, the timer while immediately return `Poll::Ready`.
    pub fn new(millis: u64) -> Self {
        Timer(tokio::spawn(async move {
            sleep(Duration::from_millis(millis)).await
        }))
    }
}

impl Future for Timer {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match Pin::new(&mut self.0).poll(cx) {
            std::task::Poll::Ready(res) => Poll::Ready(res.unwrap()),
            std::task::Poll::Pending => Poll::Pending,
        }
    }
}
