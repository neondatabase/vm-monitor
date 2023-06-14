use std::time::Duration;

use tokio::{task::JoinHandle, time::sleep};

pub struct Timer(JoinHandle<()>);

impl Timer {
    pub fn new(millis: u64) -> Self {
        Timer(tokio::spawn(async {
            sleep(Duration::from_millis(millis)).await
        }))
    }

    pub async fn stall(self) {
        self.0.await.unwrap()
    }
}
