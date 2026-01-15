mod config;
mod gmglobal_watcher;
mod market_data;
mod mock_market_data;
mod signal_processor;
mod signal_receiver;
mod trade;

use crate::trade::TradeSide;
use clap::Parser;
use config::GMGlobalConfig;
use gmglobal_watcher::GMGlobalWatcher;
use signal_processor::{process_signal, start_sl_worker};
use signal_receiver::Signal;
use tokio::sync::mpsc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    username: String,

    #[arg(short, long)]
    password: String,

    #[arg(short, long, default_value_t = 1)]
    lots: u32,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    std::env::set_var("REQWEST_DISABLE_IPV6", "1");

    println!("=== TRADING ENGINE START ===");

    // 1. Start GMGlobal watcher
    let config = GMGlobalConfig::new(args.username, args.password);
    let (watcher, subscribe_rx) = GMGlobalWatcher::new(config).await;

    let ws_watcher = watcher.clone();
    tokio::spawn(async move {
        ws_watcher.run(subscribe_rx).await;
    });

    watcher
        .as_ref()
        .load_scripts()
        .await
        .expect("Failed to load scripts");

    // 2. Startup Cleanup and Initialization
    println!("[STARTUP] Performing initial cleanup...");

    // Delete all pending orders
    match watcher.get_pending_orders().await {
        Ok(orders) => {
            for order_id in orders {
                match watcher.delete_order(&order_id).await {
                    Ok(_) => println!("[STARTUP] Deleted pending order: {}", order_id),
                    Err(e) => println!("[STARTUP] Error deleting order {}: {}", order_id, e),
                }
            }
        }
        Err(e) => println!("[STARTUP] Error fetching pending orders: {}", e),
    }

    // Place initial SL and start sl worker for all the open position
    match watcher.get_open_positions().await {
        Ok(positions) => {
            for pos in positions {
                // Assuming position structure based on expected API
                let product = pos["InstrumentIdentifier"]
                    .as_str()
                    .unwrap_or("")
                    .to_string();
                let qty = pos["Quantity"].as_u64().unwrap_or(0) as u32;
                let side_str = pos["TradeSide"].as_str().unwrap_or("").to_lowercase();
                let entry_price = pos["AveragePrice"].as_f64().unwrap_or(0.0);

                if product.is_empty() || qty == 0 {
                    continue;
                }

                let side = if side_str.contains("buy") {
                    TradeSide::Buy
                } else {
                    TradeSide::Sell
                };

                println!(
                    "[STARTUP] Starting SL worker for open position: {} {} @ {}",
                    side_str, product, entry_price
                );

                let script_id = pos["script_id"].as_str().map(|s| s.to_string());
                let script_expiry_id = pos["script_expiry_id"].as_str().map(|s| s.to_string());

                let w = watcher.clone();
                tokio::spawn(async move {
                    start_sl_worker(
                        w,
                        product,
                        entry_price,
                        side,
                        qty,
                        1, // Updated market_type_id to 1
                        script_id,
                        script_expiry_id,
                    )
                    .await;
                });
            }
        }
        Err(e) => println!("[STARTUP] Error fetching open positions: {}", e),
    }

    // 3. Start Signal Receiver
    let (signal_tx, mut signal_rx) = mpsc::channel::<Signal>(100);

    tokio::spawn(async move {
        signal_receiver::start(signal_tx).await;
    });

    let watcher_for_signals = watcher.clone();
    let lot_count = args.lots;

    tokio::spawn(async move {
        while let Some(signal) = signal_rx.recv().await {
            let w = watcher_for_signals.clone();

            // Perform script check BEFORE starting sequential logic
            if !w.has_script(&signal.product).await {
                println!(
                    "[SIGNAL] Script not found: {}. Skipping signal.",
                    signal.product
                );
                continue;
            }

            println!(
                "[SIGNAL] Processing {} for {} lots sequentially",
                signal.product, lot_count
            );

            tokio::spawn(async move {
                for i in 0..lot_count {
                    println!(
                        "[SIGNAL] Sequential Lot {}/{} starting for {}",
                        i + 1,
                        lot_count,
                        signal.product
                    );
                    let s = signal.clone();
                    process_signal(w.clone(), s).await;
                }
                println!(
                    "[SIGNAL] Completed all {} lots for {}",
                    lot_count, signal.product
                );
            });
        }
    });

    println!(
        "Trading engine running and waiting for signals (Lots: {})...",
        lot_count
    );

    // 4. Keep main alive
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}
