
#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    proxy::main().await
}
