use async_trait::async_trait;
use crate::gmglobal_watcher::Quote;

#[async_trait]
pub trait MarketData: Send + Sync {
    async fn get_quote(&self, product: &str) -> Quote;
}
