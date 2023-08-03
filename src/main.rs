#[tokio::main]
async fn main() -> anyhow::Result<()> {
    vm_monitor::main().await
}
