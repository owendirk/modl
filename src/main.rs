#[tokio::main]
async fn main() -> anyhow::Result<()> {
    modl::run_cli().await
}
