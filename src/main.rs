use clap::Parser;
use std::sync::Arc;
use tokio::sync::mpsc;
use trading_engine::config::GMGlobalConfig;
use trading_engine::gmglobal_watcher::GMGlobalWatcher;
use trading_engine::signal_processor::{process_signal, start_sl_worker};
use trading_engine::signal_receiver::Signal;
use trading_engine::trade::TradeSide;
use trading_engine::{config, gmglobal_watcher, signal_processor, signal_receiver, trade};

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
                    Err(e) => println!(
                        "[STARTUP] Failed to delete pending order {}: {}",
                        order_id, e
                    ),
                }
            }
        }
        Err(e) => println!("[STARTUP] Failed to fetch pending orders: {}", e),
    }

    // Process open positions
    match watcher.get_open_positions().await {
        Ok(positions) => {
            for pos in positions {
                let product = pos["InstrumentIdentifier"]
                    .as_str()
                    .unwrap_or("")
                    .to_string();
                let qty = pos["Quantity"].as_u64().unwrap_or(0) as u32;
                let side_str = pos["TradeSide"].as_str().unwrap_or("").to_lowercase();
                let entry_price = pos["AveragePrice"].as_f64().unwrap_or(0.0);

                let side = if side_str == "buy" {
                    TradeSide::Buy
                } else if side_str == "sell" {
                    TradeSide::Sell
                } else {
                    continue;
                };

                if qty == 0 {
                    continue;
                }

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
        Err(e) => println!("[STARTUP] Failed to fetch open positions: {}", e),
    }

    // 3. Start Signal Receiver
    let (signal_tx, mut signal_rx) = mpsc::channel::<Signal>(100);

    tokio::spawn(async move {
        signal_receiver::start(signal_tx).await;
    });

    let watcher_for_signals = watcher.clone();
    let lot_count = args.lots;

    tokio::spawn(async move {
        let w = watcher_for_signals;
        while let Some(signal) = signal_rx.recv().await {
            println!(
                "[SIGNAL] Processing {} for {} lots sequentially",
                signal.product, lot_count
            );

            let w_inner = w.clone();
            tokio::spawn(async move {
                for i in 0..lot_count {
                    println!(
                        "[SIGNAL] Sequential Lot {}/{} starting for {}",
                        i + 1,
                        lot_count,
                        signal.product
                    );
                    let s = signal.clone();
                    process_signal(w_inner.clone(), s).await;
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
