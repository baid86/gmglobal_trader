use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::{connect_async, tungstenite::Message};



#[derive(Debug, Deserialize)]
struct SignalEnvelope {
    #[serde(rename = "type")]
    event_type: String,
    data: SignalData,
}

#[derive(Debug, Deserialize)]
struct SignalData {
    symbol: String,
    signal_type: String,          // "BUY" / "SELL"
    price_at_signal: f64,
    metadata: SignalMetadata,
}

#[derive(Debug, Deserialize)]
struct SignalMetadata {
    ltp_at_signal: f64,
    lot_size: u32,
}

#[derive(Debug, Clone)]
pub enum SignalSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
pub struct Signal {
    pub product: String,
    pub price: f64,
    pub target: f64,
    pub side: SignalSide,
    pub trade_qty: u32,
}

// ---- Adjust this to match your real signal payload ----
#[derive(Debug, Deserialize)]
struct RawSignal {
    product: String,
    price: f64,
    target: f64,
    side: String, // "BUY" / "SELL"
}

fn map_symbol(symbol: &str) -> Option<String> {
    // Must be a futures symbol
    if !symbol.ends_with("FUT") {
        return None;
    }

    // Remove trailing "FUT"
    let without_fut = symbol.trim_end_matches("FUT");

    // Strip expiry (digits + month letters at the end)
    // Example: IRCTC26JAN -> IRCTC
    let underlying = without_fut
        .trim_end_matches(|c: char| c.is_ascii_alphabetic())
        .trim_end_matches(|c: char| c.is_ascii_digit());

    if underlying.is_empty() {
        return None;
    }

    Some(format!("{}-I", underlying))
}


fn parse_signal(text: &str) -> Option<Signal> {
    // Find first '{' and last '}'
    let start = text.find('{')?;
    let end = text.rfind('}')?;

    if end <= start {
        return None;
    }

    let json_part = &text[start..=end];

    let envelope: SignalEnvelope = serde_json::from_str(json_part).ok()?;

    if envelope.event_type != "signal" {
        return None;
    }

    let data = envelope.data;

    let product = map_symbol(&data.symbol)?;

    let side = match data.signal_type.to_uppercase().as_str() {
        "BUY" => SignalSide::Buy,
        "SELL" => SignalSide::Sell,
        _ => return None,
    };

    Some(Signal {
        product,
        price: data.price_at_signal,
        target: data.metadata.ltp_at_signal,
        side,
        trade_qty: data.metadata.lot_size,
    })
}


pub async fn start(tx: Sender<Signal>) {
    let url = "ws://localhost:8000/ws";

    println!("Connecting to signal WebSocket...");
    let (ws_stream, _) = connect_async(url)
        .await
        .expect("Failed to connect to signal WS");

    println!("Connected to signal WebSocket");

    let (_, mut read) = ws_stream.split();

    while let Some(msg) = read.next().await {
        if let Ok(Message::Text(text)) = msg {
            if let Some(signal) = parse_signal(&text) {
                println!("[SIGNAL RECEIVED] {:?}", signal);
                let _ = tx.send(signal).await;
            }
        }
    }

    println!("Signal WebSocket closed");
}
