mod config;
mod gmglobal_watcher;
mod trade;
mod market_data;

use config::GMGlobalConfig;
use gmglobal_watcher::GMGlobalWatcher;
use trade::{TradeSide, OrderType};

use std::sync::Arc;

#[tokio::main]
async fn main() {
    // Windows networking stability
    std::env::set_var("REQWEST_DISABLE_IPV6", "1");

    println!("=== GMGLOBAL TRADE TEST START ===");

    // --------------------------------
    // 1️⃣ Login & initialize client
    // --------------------------------
    let config = GMGlobalConfig::new();

    let (watcher, _subscribe_rx) = GMGlobalWatcher::new(config).await;
    let watcher = Arc::new(watcher);
    watcher
        .as_ref()
        .load_scripts()
        .await
        .expect("Failed to load scripts");

    println!("Logged in successfully. Attempting trade...");

    // --------------------------------
    // 2️⃣ PLACE SINGLE TEST TRADE
    // --------------------------------
    // ⚠️ REAL TRADE — qty=1
    let result = watcher
        .place_trade(
            "ASIANPAINT-I", // product
            Some(3000.0), // price (LIMIT)
            TradeSide::Buy, // Buy or Sell
            OrderType::Market,
            1,
        )
        .await;

    // --------------------------------
    // 3️⃣ PRINT RESULT
    // --------------------------------
    match result {
        Ok(json) => {
            println!("✅ TRADE RESPONSE:");
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
        }
        Err(err) => {
            println!("❌ TRADE ERROR:");
            println!("{}", err);
        }
    }

    println!("=== GMGLOBAL TRADE TEST END ===");
}
