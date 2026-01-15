use async_trait::async_trait;
use mockall::mock;
use serde_json::json;
use std::sync::Arc;
use trading_engine::gmglobal_watcher::Quote;
use trading_engine::market_data::{MarketData, TradeExecutor, Trader};
use trading_engine::signal_processor::process_signal;
use trading_engine::signal_receiver::{Signal, SignalSide};
use trading_engine::trade::{OrderType, TradeSide};

mock! {
    pub MyTrader {}
    #[async_trait]
    impl MarketData for MyTrader {
        async fn get_quote(&self, product: &str) -> Quote;
    }
    #[async_trait]
    impl TradeExecutor for MyTrader {
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
        ) -> Result<serde_json::Value, String>;
        async fn delete_order(&self, order_id: &str) -> Result<serde_json::Value, String>;
    }
}

#[tokio::test]
async fn test_process_signal_execution() {
    let mut mock = MockMyTrader::new();

    let product = "TEST-I".to_string();
    let signal = Signal {
        product: product.clone(),
        price: 100.0,
        target: 105.0,
        side: SignalSide::Buy,
        trade_qty: 1,
    };

    mock.expect_get_quote()
        .with(mockall::predicate::eq("TEST-I"))
        .times(1)
        .returning(|_| Quote {
            product: "TEST-I".into(),
            bid: 104.0,
            ask: 105.0,
            ltp: 104.5,
            timestamp: "t1".into(),
        });

    mock.expect_get_quote()
        .with(mockall::predicate::eq("TEST-I"))
        .times(1)
        .returning(|_| Quote {
            product: "TEST-I".into(),
            bid: 99.0,
            ask: 100.0,
            ltp: 99.5,
            timestamp: "t2".into(),
        });

    mock.expect_place_trade()
        .with(
            mockall::predicate::eq("TEST-I"),
            mockall::predicate::eq(Some(100.0)),
            mockall::predicate::eq(TradeSide::Buy),
            mockall::predicate::eq(OrderType::Market),
            mockall::predicate::eq(1),
            mockall::predicate::eq(1),
            mockall::predicate::always(),
            mockall::predicate::always(),
        )
        .times(1)
        .returning(|_, _, _, _, _, _, _, _| {
            Ok(json!({"status": "success", "trade": {"trade_id": 123}}))
        });

    let trader = Arc::new(mock);
    process_signal(trader, signal).await;
}

#[tokio::test]
async fn test_sl_worker_trailing() {
    let mut mock = MockMyTrader::new();
    let product = "TRAIL-I".to_string();

    mock.expect_get_quote()
        .with(mockall::predicate::eq("TRAIL-I"))
        .times(1)
        .returning(|_| Quote {
            product: "TRAIL-I".into(),
            bid: 100.0,
            ask: 101.0,
            ltp: 100.5,
            timestamp: "t1".into(),
        });

    mock.expect_place_trade()
        .with(
            mockall::predicate::eq("TRAIL-I"),
            mockall::predicate::eq(Some(99.9)),
            mockall::predicate::eq(TradeSide::Sell),
            mockall::predicate::eq(OrderType::StopLoss),
            mockall::predicate::always(),
            mockall::predicate::eq(1),
            mockall::predicate::always(),
            mockall::predicate::always(),
        )
        .times(1)
        .returning(|_, _, _, _, _, _, _, _| {
            Ok(json!({"status": "success", "trade": {"trade_id": "sl_1"}}))
        });

    mock.expect_get_quote()
        .with(mockall::predicate::eq("TRAIL-I"))
        .times(1)
        .returning(|_| Quote {
            product: "TRAIL-I".into(),
            bid: 105.0,
            ask: 106.0,
            ltp: 105.5,
            timestamp: "t2".into(),
        });

    mock.expect_delete_order()
        .with(mockall::predicate::eq("sl_1"))
        .times(1)
        .returning(|_| Ok(json!({"status": "success"})));

    mock.expect_place_trade()
        .with(
            mockall::predicate::eq("TRAIL-I"),
            mockall::predicate::eq(Some(104.90)),
            mockall::predicate::eq(TradeSide::Sell),
            mockall::predicate::eq(OrderType::StopLoss),
            mockall::predicate::always(),
            mockall::predicate::eq(1),
            mockall::predicate::always(),
            mockall::predicate::always(),
        )
        .times(1)
        .returning(|_, _, _, _, _, _, _, _| {
            Ok(json!({"status": "success", "trade": {"trade_id": "sl_2"}}))
        });

    mock.expect_get_quote()
        .with(mockall::predicate::eq("TRAIL-I"))
        .times(1)
        .returning(|_| Quote {
            product: "TRAIL-I".into(),
            bid: 110.0,
            ask: 111.0,
            ltp: 110.5,
            timestamp: "t3".into(),
        });

    mock.expect_delete_order()
        .with(mockall::predicate::eq("sl_2"))
        .times(1)
        .returning(|_| Err("Trade Already Deleted".to_string()));

    let trader = Arc::new(mock);
    let t = trader.clone();

    let handle = tokio::spawn(async move {
        trading_engine::signal_processor::start_sl_worker(
            t,
            "TRAIL-I".to_string(),
            100.0,
            TradeSide::Buy,
            1,
            1,
            None,
            None,
        )
        .await;
    });

    tokio::time::timeout(tokio::time::Duration::from_secs(30), handle)
        .await
        .expect("Worker timed out or didn't exit")
        .expect("Worker task failed");
}

#[tokio::test]
async fn test_failed_initial_sl_placement() {
    let mut mock = MockMyTrader::new();
    let product = "FAIL-I".to_string();

    mock.expect_get_quote().returning(|_| Quote {
        product: "FAIL-I".into(),
        bid: 100.0,
        ask: 101.0,
        ltp: 100.5,
        timestamp: "t1".into(),
    });

    // Expect 10 attempts for initial SL
    mock.expect_place_trade()
        .times(10)
        .returning(|_, _, _, _, _, _, _, _| Err("Internal Server Error".to_string()));

    let trader = Arc::new(mock);

    // This should return after 10 attempts (taking some time due to sleeps)
    // We can't easily skip sleeps in unit tests unless we mock time, but let's see.
    // Actually, start_sl_worker sleeps 2s between initial attempts.
    // 10 attempts * 2s = 20s. Integration test might take too long.

    trading_engine::signal_processor::start_sl_worker(
        trader,
        "FAIL-I".to_string(),
        100.0,
        TradeSide::Buy,
        1,
        1,
        None,
        None,
    )
    .await;
}

#[tokio::test]
async fn test_sell_signal_execution() {
    let mut mock = MockMyTrader::new();

    let product = "SELL-I".to_string();
    let signal = Signal {
        product: product.clone(),
        price: 150.0,
        target: 145.0,
        side: SignalSide::Sell,
        trade_qty: 2,
    };

    // First quote: too low (bid=145)
    mock.expect_get_quote()
        .with(mockall::predicate::eq("SELL-I"))
        .times(1)
        .returning(|_| Quote {
            product: "SELL-I".into(),
            bid: 145.0,
            ask: 146.0,
            ltp: 145.5,
            timestamp: "t1".into(),
        });

    // Second quote: hit (bid=150)
    mock.expect_get_quote()
        .with(mockall::predicate::eq("SELL-I"))
        .times(1)
        .returning(|_| Quote {
            product: "SELL-I".into(),
            bid: 151.0,
            ask: 152.0,
            ltp: 151.5,
            timestamp: "t2".into(),
        });

    mock.expect_place_trade()
        .with(
            mockall::predicate::eq("SELL-I"),
            mockall::predicate::eq(Some(151.0)),
            mockall::predicate::eq(TradeSide::Sell),
            mockall::predicate::eq(OrderType::Market),
            mockall::predicate::eq(2),
            mockall::predicate::eq(1),
            mockall::predicate::always(),
            mockall::predicate::always(),
        )
        .times(1)
        .returning(|_, _, _, _, _, _, _, _| {
            Ok(json!({"status": "success", "trade": {"trade_id": 456}}))
        });

    let trader = Arc::new(mock);
    process_signal(trader, signal).await;
}
