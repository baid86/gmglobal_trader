use std::time::Duration;
use crate::gmglobal_watcher::GMGlobalWatcher;
use crate::signal_receiver::{Signal, SignalSide};
use crate::trade::{TradeSide, OrderType};
use crate::market_data::MarketData;


fn calculate_delta(price: f64) -> f64 {
    if price < 100.0 {
        0.02
    } else {
        let raw_delta = price * 0.0002; // 0.02%
        let rounded = round_to_5_paisa(raw_delta);
        rounded.max(0.05) // enforce minimum tick
    }
}

fn round_to_5_paisa(value: f64) -> f64 {
    (value / 0.05).round() * 0.05
}

pub async fn process_signal(
    watcher: std::sync::Arc<GMGlobalWatcher>,
    signal: Signal,
) {
    println!("[SIGNAL] Processing {:?}", signal);

    let threshold = calculate_delta(signal.price);

    loop {
        let quote = watcher.get_quote(&signal.product).await;

        let entry_hit = match signal.side {
            SignalSide::Buy => {
                quote.ask <= signal.price + threshold
            }
            SignalSide::Sell => {
                quote.bid >= signal.price - threshold
            }
        };

        if entry_hit {
            match signal.side {
                SignalSide::Buy => {
                    println!(
                        "[ENTRY] BUY {} @ ask={} signal={} threshold={}",
                        signal.product, quote.ask, signal.price, threshold
                    );

                    let result = watcher
                        .place_trade(
                            signal.product.as_str(),
                            Some(signal.target),
                            TradeSide::Buy,
                            OrderType::Market,
                            signal.trade_qty,
                        )
                        .await;

                    match result {
                        Ok(_) => {
                            println!("[TRADE] BUY trade placed for {}", signal.product);
                            let product = signal.product.clone();
                            // 🔧 SL worker placeholder
                            tokio::spawn(async move {
                                println!("[SL_WORKER] Started for {}", product);
                                // TODO: implement SL logic
                            });

                            break; // 🚨 EXIT LOOP AFTER ENTRY
                        }
                        Err(err) => {
                            println!("[TRADE] Error placing BUY trade: {}", err);
                            break;
                        }
                    }
                }

                SignalSide::Sell => {
                    println!(
                        "[ENTRY] SELL {} @ bid={} signal={} threshold={}",
                        signal.product, quote.bid, signal.price, threshold
                    );

                    let result = watcher
                        .place_trade(
                            signal.product.as_str(),
                            Some(signal.target),
                            TradeSide::Sell,
                            OrderType::Market,
                            signal.trade_qty,
                        )
                        .await;

                    match result {
                        Ok(_) => {
                            println!("[TRADE] SELL trade placed for {}", signal.product);

                            // 🔧 SL worker placeholder
                            let product = signal.product.clone();
                            tokio::spawn(async move {
                                println!("[SL_WORKER] Started for {}", product);
                                // TODO: implement SL logic
                            });

                            break; // 🚨 EXIT LOOP AFTER ENTRY
                        }
                        Err(err) => {
                            println!("[TRADE] Error placing SELL trade: {}", err);
                            break;
                        }
                    }
                }
            }
        }

        // throttle loop
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    println!("[SIGNAL] Completed {}", signal.product);
}
