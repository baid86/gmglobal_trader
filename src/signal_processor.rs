use crate::market_data::Trader;
use crate::signal_receiver::{Signal, SignalSide};
use crate::trade::{OrderType, TradeSide};
use std::sync::Arc;
use std::time::Duration;

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

pub async fn process_signal<T: Trader + 'static>(watcher: Arc<T>, signal: Signal) {
    println!("[SIGNAL] Processing {:?}", signal);

    let threshold = calculate_delta(signal.price);

    loop {
        let quote = watcher.get_quote(&signal.product).await;

        let entry_hit = match signal.side {
            SignalSide::Buy => quote.ask <= signal.price + threshold,
            SignalSide::Sell => quote.bid >= signal.price - threshold,
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
                            Some(quote.ask), // Using current ask for Market Buy
                            TradeSide::Buy,
                            OrderType::Market,
                            signal.trade_qty,
                            1, // market_type_id
                            None,
                            None,
                        )
                        .await;

                    match result {
                        Ok(_) => {
                            println!("[TRADE] BUY trade placed for {}", signal.product);
                            let product = signal.product.clone();
                            // 🔧 SL worker
                            let w = watcher.clone();
                            tokio::spawn(async move {
                                start_sl_worker(
                                    w,
                                    product,
                                    signal.price,
                                    TradeSide::Buy,
                                    signal.trade_qty,
                                    1, // market_type_id
                                    None,
                                    None,
                                )
                                .await;
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
                            Some(quote.bid), // Using current bid for Market Sell
                            TradeSide::Sell,
                            OrderType::Market,
                            signal.trade_qty,
                            1, // market_type_id
                            None,
                            None,
                        )
                        .await;

                    match result {
                        Ok(_) => {
                            println!("[TRADE] SELL trade placed for {}", signal.product);

                            // 🔧 SL worker
                            let w = watcher.clone();
                            let product = signal.product.clone();
                            tokio::spawn(async move {
                                start_sl_worker(
                                    w,
                                    product,
                                    signal.price,
                                    TradeSide::Sell,
                                    signal.trade_qty,
                                    1, // market_type_id
                                    None,
                                    None,
                                )
                                .await;
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

pub async fn start_sl_worker<T: Trader + 'static>(
    watcher: Arc<T>,
    product: String,
    _signal_price: f64,
    entry_side: TradeSide,
    qty: u32,
    market_type_id: u8,
    script_id: Option<String>,
    script_expiry_id: Option<String>,
) {
    println!(
        "[SL_WORKER] [{}] Starting worker for {} qty (Side: {:?})",
        product, qty, entry_side
    );

    // The SL order side is the opposite of entry side
    let sl_side = match entry_side {
        TradeSide::Buy => TradeSide::Sell,
        TradeSide::Sell => TradeSide::Buy,
    };

    // 1. Place initial SL
    let current_quote = watcher.get_quote(&product).await;

    // Calculate initial base price
    let base_price = match entry_side {
        TradeSide::Buy => current_quote.bid.min(current_quote.ltp),
        TradeSide::Sell => current_quote.ask.max(current_quote.ltp),
    };

    let raw_sl = match entry_side {
        TradeSide::Buy => base_price * 0.999,
        TradeSide::Sell => base_price * 1.001,
    };
    let sl_price = round_to_5_paisa(raw_sl);

    println!(
        "[SL_WORKER] [{}] Placing initial SL at {} (LTP: {}, Base: {})",
        product, sl_price, current_quote.ltp, base_price
    );

    let mut current_sl_id = None;
    let mut attempts = 0;
    let max_initial_retries = 10;
    while attempts < max_initial_retries {
        match watcher
            .place_trade(
                &product,
                Some(sl_price),
                sl_side,
                OrderType::StopLoss,
                qty,
                market_type_id,
                script_id.clone(),
                script_expiry_id.clone(),
            )
            .await
        {
            Ok(resp) => {
                let id = resp["trade"]["trade_id"]
                    .as_i64()
                    .map(|id| id.to_string())
                    .or_else(|| resp["trade"]["trade_id"].as_str().map(|s| s.to_string()))
                    .unwrap_or_else(|| "0".to_string());
                println!("[SL_WORKER] [{}] Initial SL placed. ID: {}", product, id);
                current_sl_id = Some(id);
                break;
            }
            Err(e) => {
                attempts += 1;
                println!(
                    "[SL_WORKER] [{}] Initial SL placement attempt {}/{} failed: {}. Retrying in 2s...",
                    product, attempts, max_initial_retries, e
                );
                if attempts >= max_initial_retries {
                    println!(
                        "[SL_WORKER] [{}] FATAL: Failed to place initial SL after {} attempts.",
                        product, max_initial_retries
                    );
                    return;
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    let mut current_sl_price = Some(sl_price);
    let mut consecutive_failures = 0;
    let max_consecutive_failures = 10; // User requested 10 retries

    loop {
        let quote = watcher.get_quote(&product).await;

        // Calculate potential SL price based on .1% less than min(bid, ltp)
        // For SELL entry, the SL should be HIGHER than price: max(ask, ltp) + .1%
        let base_price = match entry_side {
            TradeSide::Buy => quote.bid.min(quote.ltp),
            TradeSide::Sell => quote.ask.max(quote.ltp),
        };

        let raw_sl = match entry_side {
            TradeSide::Buy => base_price * 0.999,
            TradeSide::Sell => base_price * 1.001,
        };

        let new_sl_price = round_to_5_paisa(raw_sl);

        let should_update = match current_sl_price {
            None => true, // Should not happen after initial placement
            Some(curr) => match entry_side {
                TradeSide::Buy => new_sl_price >= curr + 0.05, // Only move UP
                TradeSide::Sell => new_sl_price <= curr - 0.05, // Only move DOWN
            },
        };

        if should_update {
            // 1. Delete old SL if exists
            if let Some(old_id) = current_sl_id.take() {
                println!("[SL_WORKER] [{}] Deleting old SL {}", product, old_id);

                let mut del_attempts = 0;
                let mut deleted = false;
                let max_del_attempts = 10;
                while del_attempts < max_del_attempts {
                    match watcher.delete_order(&old_id).await {
                        Ok(_) => {
                            deleted = true;
                            break;
                        }
                        Err(e) => {
                            let err_msg = e.to_lowercase();
                            if err_msg.contains("already")
                                || err_msg.contains("not found")
                                || err_msg.contains("executed")
                            {
                                println!(
                                    "[SL_WORKER] [{}] Order {} appears to be already processed or gone ({}). Exiting worker.",
                                    product, old_id, e
                                );
                                return;
                            }

                            del_attempts += 1;
                            println!(
                                "[SL_WORKER] [{}] Deletion attempt {}/{} failed for {}: {}. Retrying...",
                                product, del_attempts, max_del_attempts, old_id, e
                            );
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }

                if !deleted {
                    consecutive_failures += 1;
                    println!(
                        "[SL_WORKER] [{}] Deletion failed after {} attempts for {}. Consecutive failures: {}/{}",
                        product, max_del_attempts, old_id, consecutive_failures, max_consecutive_failures
                    );

                    if consecutive_failures >= max_consecutive_failures {
                        println!("[SL_WORKER] [{}] FATAL: Too many consecutive failures during deletion. Exiting worker.", product);
                        return;
                    }
                    continue;
                }
                consecutive_failures = 0;
            }

            // 2. Place new SL
            println!(
                "[SL_WORKER] [{}] Placing new trailing SL for {} @ {}",
                product, product, new_sl_price
            );
            let mut place_attempts = 0;
            let mut placed = false;
            let max_place_attempts = 10;
            while place_attempts < max_place_attempts {
                match watcher
                    .place_trade(
                        &product,
                        Some(new_sl_price),
                        sl_side,
                        OrderType::StopLoss,
                        qty,
                        market_type_id,
                        script_id.clone(),
                        script_expiry_id.clone(),
                    )
                    .await
                {
                    Ok(resp) => {
                        let trade_id = resp["trade"]["trade_id"]
                            .as_i64()
                            .map(|id| id.to_string())
                            .or_else(|| resp["trade"]["trade_id"].as_str().map(|s| s.to_string()));

                        if let Some(id) = trade_id {
                            current_sl_id = Some(id);
                            current_sl_price = Some(new_sl_price);
                            println!(
                                "[SL_WORKER] [{}] New SL placed: {} @ {}",
                                product,
                                current_sl_id.as_ref().unwrap(),
                                new_sl_price
                            );
                            placed = true;
                            break;
                        } else {
                            println!(
                                "[SL_WORKER] [{}] Warning: No trade_id in SL response",
                                product
                            );
                            break; // Don't retry if we got a success but no ID
                        }
                    }
                    Err(e) => {
                        place_attempts += 1;
                        println!(
                            "[SL_WORKER] [{}] Trailing SL placement attempt {}/{} failed: {}. Retrying...",
                            product, place_attempts, max_place_attempts, e
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }

            if !placed && place_attempts >= max_place_attempts {
                consecutive_failures += 1;
                println!(
                    "[SL_WORKER] [{}] SL placement failed after {} attempts. Consecutive failures: {}/{}",
                    product, max_place_attempts, consecutive_failures, max_consecutive_failures
                );

                if consecutive_failures >= max_consecutive_failures {
                    println!(
                        "[SL_WORKER] [{}] FATAL: Too many consecutive failures. Exiting worker.",
                        product
                    );
                    return;
                }
                continue; // Retry next loop instead of hard return
            }

            if placed {
                consecutive_failures = 0; // Reset on success
            }
        }

        tokio::time::sleep(Duration::from_secs(6)).await;
    }
}
