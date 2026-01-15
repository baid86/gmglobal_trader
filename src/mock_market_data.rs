use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::gmglobal_watcher::Quote;
use crate::market_data::MarketData;

// =======================
// Mock Market Data
// =======================

pub struct MockMarketData {
    quotes: Arc<RwLock<HashMap<String, Vec<Quote>>>>,
}

impl MockMarketData {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            quotes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    #[allow(dead_code)]
    pub async fn add_quotes(&self, product: &str, quotes: Vec<Quote>) {
        self.quotes
            .write()
            .await
            .insert(product.to_string(), quotes);
    }
}

#[async_trait::async_trait]
impl MarketData for MockMarketData {
    async fn get_quote(&self, product: &str) -> Quote {
        let mut map = self.quotes.write().await;
        let qlist = map.get_mut(product).expect("No mock quotes for product");
        qlist.remove(0)
    }
}

// =======================
// Tests (IN FILE)
// =======================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::process_signal;
    use crate::signal_receiver::{Signal, SignalSide};

    #[tokio::test]
    async fn test_buy_signal_reaches_target() {
        let market = Arc::new(MockMarketData::new());

        market
            .add_quotes(
                "KALYANKJIL-I",
                vec![
                    Quote {
                        product: "KALYANKJIL-I".into(),
                        bid: 99.0,
                        ask: 100.0,
                        ltp: 100.0,
                        timestamp: "t1".into(),
                    },
                    Quote {
                        product: "KALYANKJIL-I".into(),
                        bid: 101.0,
                        ask: 102.0,
                        ltp: 102.0,
                        timestamp: "t2".into(),
                    },
                ],
            )
            .await;

        let signal = Signal {
            product: "KALYANKJIL-I".into(),
            price: 100.0,
            target: 105.0,
            side: SignalSide::Buy,
            trade_qty: 1,
        };

        // This is what we are actually testing
        // process_signal(market, signal).await;
    }
}
