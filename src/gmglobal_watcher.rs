use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, cookie::Jar};
use serde::{Deserialize};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{RwLock, Notify, mpsc};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::config::GMGlobalConfig;
use crate::market_data::MarketData;
use crate::trade::{TradeRequest, TradeSide, OrderType};

#[derive(Debug, Clone)]
struct ScriptInfo {
    script_id: String,
    script_expiry_id: String,
}

#[derive(Debug, Clone)]
pub struct Quote {
    pub product: String,
    pub ltp: f64,
    pub bid: f64,
    pub ask: f64,
    pub timestamp: String,
}

pub struct GMGlobalWatcher {
    client: Client,
    config: GMGlobalConfig,
    user_id: String,

    quotes: Arc<RwLock<HashMap<String, Quote>>>,
    notifiers: Arc<RwLock<HashMap<String, Arc<Notify>>>>,

    subscribe_tx: mpsc::Sender<String>,
    scripts: RwLock<HashMap<String, ScriptInfo>>,
}

#[async_trait::async_trait]
impl MarketData for GMGlobalWatcher {
    async fn get_quote(&self, product: &str) -> Quote {
        self.get_quote_internal(product).await
    }
}

impl GMGlobalWatcher {
    /* ===================== INIT ===================== */

    pub async fn new(config: GMGlobalConfig) -> (Arc<Self>, mpsc::Receiver<String>) {
        let jar = Arc::new(Jar::default());

        let client = Client::builder()
            .cookie_provider(jar)
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true)
            .timeout(Duration::from_secs(15))
            .build()
            .expect("HTTP client build failed");

        let user_id = Self::login(&client, &config).await;

        let (subscribe_tx, subscribe_rx) = mpsc::channel(256);

        let watcher = Arc::new(Self {
            client,
            config,
            user_id,
            quotes: Arc::new(RwLock::new(HashMap::new())),
            notifiers: Arc::new(RwLock::new(HashMap::new())),
            subscribe_tx,
            scripts: RwLock::new(HashMap::new()),
        });

        (watcher, subscribe_rx)
    }

    async fn login(client: &Client, config: &GMGlobalConfig) -> String {
        let resp = client
            .post(&config.login_url)
            .json(&serde_json::json!({
                "username": config.username,
                "password": config.password
            }))
            .send()
            .await
            .expect("Login request failed");

        let json: Value = resp.json().await.expect("Invalid login response");

        json["user_id"]
            .as_str()
            .expect("user_id missing")
            .to_string()
    }

    /* ===================== QUOTES ===================== */

    pub async fn get_quote_internal(&self, product: &str) -> Quote {
        if let Some(q) = self.quotes.read().await.get(product).cloned() {
            return q;
        }

        let notify = {
            let mut map = self.notifiers.write().await;
            map.entry(product.to_string())
                .or_insert_with(|| Arc::new(Notify::new()))
                .clone()
        };

        let _ = self.subscribe_tx.send(product.to_string()).await;

        loop {
            if let Some(q) = self.quotes.read().await.get(product).cloned() {
                return q;
            }
            notify.notified().await;
        }
    }

    /* ===================== RUN LOOP ===================== */

    pub async fn run(self: Arc<Self>, subscribe_rx: mpsc::Receiver<String>) {
        let mut rx = subscribe_rx;
        let mut active_products = HashSet::new();

        loop {
            println!("[WS] starting new session");

            if let Err(e) = self.run_ws(&mut rx, &mut active_products).await {
                println!("[WS] error: {} — reconnecting", e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }

    async fn run_ws(
        &self,
        subscribe_rx: &mut mpsc::Receiver<String>,
        active: &mut HashSet<String>,
    ) -> Result<(), String> {

        let sid = self.polling_handshake().await?;

        let ws_url = format!(
            "wss://thedatamining.org:4003/socket.io/?EIO=3&transport=websocket&sid={}",
            sid
        );

        let (ws, _) = connect_async(ws_url)
            .await
            .map_err(|e| e.to_string())?;

        let (mut write, mut read) = ws.split();

        write.send(Message::Text("2probe".into())).await.map_err(|e| e.to_string())?;
        if let Some(Ok(Message::Text(text))) = read.next().await {
            if text == "3probe" {
                write
                    .send(Message::Text("5".into()))
                    .await
                    .map_err(|e| e.to_string())?;
            }
        }

        for p in active.iter() {
            let msg = format!(r#"42["addMarketWatch",{{"product":"{}"}}]"#, p);
            let _ = write.send(Message::Text(msg)).await;
        }

        loop {
            tokio::select! {
                msg = read.next() => {
                    println!("[WS] Message: {:?}", msg);
                    let Some(Ok(Message::Text(text))) = msg else {
                        return Err("WS closed".into());
                    };

                    if text == "2" {
                        let _ = write.send(Message::Text("3".into())).await;
                        continue;
                    }

                    if text.starts_with("42") {
                        // Extract only the JSON array part: [ ... ]
                        if let (Some(start), Some(end)) = (text.find('['), text.rfind(']')) {
                            let json_part = &text[start..=end];
                    
                            let data: Value = match serde_json::from_str(json_part) {
                                Ok(v) => v,
                                Err(_) => continue, // 🚨 DO NOT reconnect on bad frame
                            };
                    
                            let Some(event) = data.get(0).and_then(|v| v.as_str()) else { continue };
                            if event != "marketWatch" {
                                continue;
                            }
                    
                            let Some(q) = data.get(1).and_then(|v| v.get("data")) else { continue };
                            let Some(product) = q.get("InstrumentIdentifier").and_then(|v| v.as_str()) else { continue };
                    
                            let quote = Quote {
                                product: product.to_string(),
                                ltp: q.get("LastTradePrice").and_then(|v| v.as_f64()).unwrap_or(0.0),
                                bid: q.get("BuyPrice").and_then(|v| v.as_f64()).unwrap_or(0.0),
                                ask: q.get("SellPrice").and_then(|v| v.as_f64()).unwrap_or(0.0),
                                timestamp: chrono::Utc::now().to_rfc3339(),
                            };
                    
                            self.quotes.write().await.insert(product.to_string(), quote);
                    
                            if let Some(n) = self.notifiers.write().await.remove(product) {
                                n.notify_waiters();
                            }
                        }
                    }
                }

                Some(product) = subscribe_rx.recv() => {
                    if active.insert(product.clone()) {
                        let msg = format!(r#"42["addMarketWatch",{{"product":"{}"}}]"#, product);
                        if write.send(Message::Text(msg)).await.is_err() {
                            return Err("WS send failed".into());
                        }
                        println!("[SUBSCRIBE] {}", product);
                    }
                }
            }
        }
    }

    /* ===================== HANDSHAKE ===================== */

    async fn polling_handshake(&self) -> Result<String, String> {
        let resp = self.client
            .get(format!("{}?EIO=3&transport=polling", self.config.socket_base))
            .send()
            .await
            .map_err(|e| e.to_string())?;
    
        let body = resp.text().await.map_err(|e| e.to_string())?;
        println!("[WS] Handshake response: {}", body);
        // Extract ONLY the JSON object { ... }
        let start = body.find('{').ok_or("No JSON start")?;
    
        let mut depth = 0usize;
        let mut end = None;
    
        for (i, ch) in body[start..].char_indices() {
            match ch {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(start + i + 1);
                        break;
                    }
                }
                _ => {}
            }
        }
    
        let end = end.ok_or("No JSON end")?;
        let json_part = &body[start..end];
    
        let json: Value = serde_json::from_str(json_part)
            .map_err(|e| format!("Handshake JSON parse failed: {}", e))?;
    
        let sid = json["sid"]
            .as_str()
            .ok_or("SID missing in handshake")?
            .to_string();
    
        Ok(sid)
    }
    

    /* ===================== TRADING ===================== */

    pub async fn place_trade(
        &self,
        product: &str,
        price: Option<f64>,
        side: TradeSide,
        order_type: OrderType,
        trade_qty: u32,
    ) -> Result<Value, String> {

        let script = self
            .scripts
            .read()
            .await
            .get(product)
            .cloned()
            .ok_or_else(|| format!("Script not found: {}", product))?;

        let mut payload = TradeRequest::new(product, price, side, order_type, trade_qty);
        payload.script_id = Some(script.script_id);
        payload.script_expiry_id = Some(script.script_expiry_id);

        let resp = self.client
            .post("https://www.gmglobal.org/ajaxfiles/trade_place.php")
            .json(&payload)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let text = resp.text().await.map_err(|e| e.to_string())?;
        serde_json::from_str(&text).map_err(|e| e.to_string())
    }

    pub async fn load_scripts(&self) -> Result<(), String> {
        let url = "https://www.gmglobal.org/ajaxfiles/market_watch_list1.php";

        println!("[GMGLOBAL] Loading script master list...");

        let resp = self.client
            .post(url)
            .json(&serde_json::json!({}))
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let body = resp.text().await.map_err(|e| e.to_string())?;

        let parsed: MarketWatchResponse =
            serde_json::from_str(&body).map_err(|e| {
                format!("Failed to parse script list: {} | {}", e, body)
            })?;

        let mut map = HashMap::new();

        for s in parsed.scripts {
            let product = format!("{}-{}", s.script_name, s.script_expiry_type);

            map.insert(
                product,
                ScriptInfo {
                    script_id: s.script_id,
                    script_expiry_id: s.script_expiry_id,
                },
            );
        }

        let count = map.len();
        *self.scripts.write().await = map;

        println!("[GMGLOBAL] Loaded {} scripts", count);
        Ok(())
    }
}

/* ===================== SCRIPT MASTER ===================== */

#[derive(Debug, Deserialize)]
struct MarketWatchResponse {
    scripts: Vec<MarketWatchScript>,
}

#[derive(Debug, Deserialize)]
struct MarketWatchScript {
    script_id: String,
    script_name: String,
    script_expiry_id: String,
    script_expiry_type: String,
}
