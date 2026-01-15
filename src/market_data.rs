use crate::gmglobal_watcher::Quote;
use async_trait::async_trait;

use crate::trade::{OrderType, TradeSide};
use serde_json::Value;

#[async_trait]
pub trait MarketData: Send + Sync {
    async fn get_quote(&self, product: &str) -> Quote;
}

#[async_trait]
pub trait TradeExecutor: Send + Sync {
    async fn place_trade(
        &self,
        product: &str,
        price: Option<f64>,
        side: TradeSide,
        order_type: OrderType,
        trade_qty: u32,
        market_type_id: u8,
        script_id: Option<String>,
        script_expiry_id: Option<String>,
    ) -> Result<Value, String>;

    async fn delete_order(&self, order_id: &str) -> Result<Value, String>;

    async fn get_open_positions(&self) -> Result<Vec<Value>, String>;
}

pub trait Trader: MarketData + TradeExecutor {}
impl<T: MarketData + TradeExecutor> Trader for T {}
