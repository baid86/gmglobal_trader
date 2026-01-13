mod config;
mod gmglobal_watcher;
mod signal_receiver;
mod market_data;
mod trade;
mod mock_market_data;
mod signal_processor;

use config::GMGlobalConfig;
use gmglobal_watcher::GMGlobalWatcher;
use signal_receiver::Signal;
use signal_processor::process_signal;

use tokio::sync::mpsc;
use std::time::Duration;

#[tokio::main]
async fn main() {
    std::env::set_var("REQWEST_DISABLE_IPV6", "1");

    println!("=== TRADING ENGINE START ===");

    // --------------------------------
    // 1️⃣ Start GMGlobal watcher
    // --------------------------------
    let config = GMGlobalConfig::new();

    // IMPORTANT:
    // watcher is ALREADY Arc<GMGlobalWatcher>
    let (watcher, subscribe_rx) = GMGlobalWatcher::new(config).await;

    let ws_watcher = watcher.clone();
    tokio::spawn(async move {
        ws_watcher.run(subscribe_rx).await;
    });

    watcher.as_ref().load_scripts().await.expect("Failed to load scripts");
 
    // --------------------------------
    // 2️⃣ Start Signal Receiver
    // --------------------------------
    let (signal_tx, mut signal_rx) = mpsc::channel::<Signal>(100);

    tokio::spawn(async move {
        signal_receiver::start(signal_tx).await;
    });

    // --------------------------------
    // 3️⃣ Signal Dispatcher
    // --------------------------------
    let watcher_for_signals = watcher.clone();

    tokio::spawn(async move {
        while let Some(signal) = signal_rx.recv().await {
            let w = watcher_for_signals.clone();
            tokio::spawn(async move {
                process_signal(w, signal).await;
            });
        }
    });

    println!("Trading engine running...");

    // --------------------------------
    // 4️⃣ Keep main alive
    // --------------------------------
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}
