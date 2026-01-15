use futures_util::{SinkExt, StreamExt};
use reqwest::{cookie::Jar, Client};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{mpsc, Mutex, Notify, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::config::GMGlobalConfig;
use crate::market_data::MarketData;
use crate::trade::{OrderType, TradeRequest, TradeSide};

#[derive(Debug, Clone)]
struct ScriptInfo {
    script_id: String,
    script_expiry_id: String,
}

#[derive(Debug, Clone)]
pub struct Quote {
    #[allow(dead_code)]
    pub product: String,
    pub ltp: f64,
    pub bid: f64,
    pub ask: f64,
    #[allow(dead_code)]
    pub timestamp: String,
}

pub struct GMGlobalWatcher {
    client: RwLock<Client>,
    config: GMGlobalConfig,
    user_id: RwLock<String>,
    login_mutex: Mutex<()>,
    last_login_time: RwLock<Option<chrono::DateTime<chrono::Utc>>>,

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

        let (subscribe_tx, subscribe_rx) = mpsc::channel(256);

        let watcher = Arc::new(Self {
            client: RwLock::new(client),
            config,
            user_id: RwLock::new("".to_string()),
            login_mutex: Mutex::new(()),
            last_login_time: RwLock::new(None),
            quotes: Arc::new(RwLock::new(HashMap::new())),
            notifiers: Arc::new(RwLock::new(HashMap::new())),
            subscribe_tx,
            scripts: RwLock::new(HashMap::new()),
        });

        // Perform initial login
        if let Err(e) = watcher.perform_login(false).await {
            println!("[WATCHER] Initial login failed: {}", e);
        }

        (watcher, subscribe_rx)
    }

    pub async fn perform_login(&self, force: bool) -> Result<String, String> {
        let _guard = self.login_mutex.lock().await;

        // Check if we logged in recently (last 5 seconds)
        // If "force" is true, we skip this check to ensure a fresh session
        if !force {
            if let Some(last) = *self.last_login_time.read().await {
                if chrono::Utc::now().signed_duration_since(last) < chrono::Duration::seconds(5) {
                    println!("[WATCHER] Already logged in recently, using existing session.");
                    return Ok(self.user_id.read().await.clone());
                }
            }
        }

        println!("[WATCHER] Establishing fresh session (Force: {})...", force);

        // Build a COMPLETELY NEW client and jar
        let jar = Arc::new(Jar::default());
        let new_client = Client::builder()
            .cookie_provider(jar)
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true)
            .timeout(Duration::from_secs(15))
            .build()
            .expect("HTTP client build failed during rotation");

        println!("[WATCHER] Logging in to GMGlobal with fresh client...");
        let resp = new_client
            .post(&self.config.login_url)
            .json(&serde_json::json!({
                "username": self.config.username,
                "password": self.config.password
            }))
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let json: Value = resp.json().await.map_err(|e| e.to_string())?;

        let uid = json["user_id"]
            .as_str()
            .ok_or("user_id missing in login response")?
            .to_string();

        // Critical: Update both user_id and the shared client
        *self.user_id.write().await = uid.clone();
        *self.client.write().await = new_client;
        *self.last_login_time.write().await = Some(chrono::Utc::now());

        println!(
            "[WATCHER] Login successful. UserID: {}. Client rotated.",
            uid
        );
        Ok(uid)
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
            println!("[WATCHER] Starting new Websocket session...");

            if let Err(e) = self.run_ws(&mut rx, &mut active_products).await {
                println!("[WATCHER] WS error: {} — reconnecting", e);
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

        let (ws, _) = connect_async(ws_url).await.map_err(|e| e.to_string())?;

        let (mut write, mut read) = ws.split();

        write
            .send(Message::Text("2probe".into()))
            .await
            .map_err(|e| e.to_string())?;
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

        let mut heartbeat = tokio::time::interval(Duration::from_secs(25));

        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    if let Err(e) = write.send(Message::Text("2".into())).await {
                        println!("[WATCHER] Heartbeat send failed: {}", e);
                        return Err("Heartbeat failed".into());
                    }
                }

                msg = read.next() => {
                    let Some(Ok(Message::Text(text))) = msg else {
                        return Err("WS closed".into());
                    };

                    if text == "2" {
                        let _ = write.send(Message::Text("3".into())).await;
                        continue;
                    }

                    if text == "3" {
                        continue; // Pong received
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
        let http = self.client.read().await;
        let resp = http
            .get(format!(
                "{}?EIO=3&transport=polling",
                self.config.socket_base
            ))
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let body = resp.text().await.map_err(|e| e.to_string())?;
        // println!("[WS] Handshake response: {}", body);
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
        market_type_id: u8,
        script_id: Option<String>,
        script_expiry_id: Option<String>,
    ) -> Result<Value, String> {
        let mut retry_count = 0;

        loop {
            let mut payload =
                TradeRequest::new(product, price, side, order_type, trade_qty, market_type_id);

            if script_id.is_some() && script_expiry_id.is_some() {
                payload.script_id = script_id.clone();
                payload.script_expiry_id = script_expiry_id.clone();
            } else {
                let script = self
                    .scripts
                    .read()
                    .await
                    .get(product)
                    .cloned()
                    .ok_or_else(|| format!("Script not found: {}", product))?;
                payload.script_id = Some(script.script_id);
                payload.script_expiry_id = Some(script.script_expiry_id);
            }

            let uid = self.user_id.read().await.clone();
            payload.user_id = uid.clone();

            println!(
                "[WATCHER] Placing trade for {} (UserID: {})...",
                product, uid
            );
            let http = self.client.read().await;
            let resp_result = http
                .post("https://www.gmglobal.org/ajaxfiles/trade_place.php")
                .json(&payload)
                .send()
                .await;

            drop(http); // Release read lock before processing potentially recursive login

            let resp = resp_result.map_err(|e| e.to_string())?;
            let text = resp.text().await.map_err(|e| e.to_string())?;
            let json: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;

            let status = json["status"].as_str().unwrap_or("").to_lowercase();
            let message = json["message"].as_str().unwrap_or("");

            if json["status"] != "success"
                && !message.contains("Placed Successfully")
                && !status.contains("success")
            {
                if (message.contains("Invalid Login") || message.contains("Invalid Server Time"))
                    && retry_count < 2
                {
                    retry_count += 1;
                    println!(
                        "[WATCHER] {} detected. Re-logging (Attempt {})...",
                        message, retry_count
                    );
                    if let Err(e) = self.perform_login(true).await {
                        return Err(format!("Re-login failed during place_trade: {}", e));
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue; // Retry the loop
                }
                return Err(message.to_string());
            }

            return Ok(json);
        }
    }

    pub async fn delete_order(&self, order_id: &str) -> Result<Value, String> {
        self.delete_order_with_retry(order_id, true).await
    }

    pub async fn delete_order_with_retry(
        &self,
        order_id: &str,
        retry_on_session_error: bool,
    ) -> Result<Value, String> {
        let mut retry_count = 0;
        let url = "https://www.gmglobal.org/ajaxfiles/trade_delete.php";

        let trade_id = order_id
            .parse::<i64>()
            .map_err(|_| "Invalid order_id format".to_string())?;

        loop {
            let uid = self.user_id.read().await.clone();
            println!("[WATCHER] Deleting order {} (UserID: {})...", trade_id, uid);
            let http = self.client.read().await;
            let resp_result = http
                .post(url)
                .json(&serde_json::json!({
                    "trade_id": trade_id,
                    "password": ""
                }))
                .send()
                .await;

            drop(http); // Release read lock

            let resp = resp_result.map_err(|e| {
                println!(
                    "[WATCHER] Delete order {} - HTTP request failed: {}",
                    trade_id, e
                );
                e.to_string()
            })?;

            println!(
                "[WATCHER] Delete order {} - Received response, reading body...",
                trade_id
            );
            let text = resp.text().await.map_err(|e| {
                println!(
                    "[WATCHER] Delete order {} - Failed to read response body: {}",
                    trade_id, e
                );
                e.to_string()
            })?;

            println!(
                "[WATCHER] Delete order {} - Response body: {}",
                trade_id, text
            );
            let json: Value = serde_json::from_str(&text).map_err(|e| {
                println!(
                    "[WATCHER] Delete order {} - Failed to parse JSON: {}",
                    trade_id, e
                );
                e.to_string()
            })?;

            if json["status"] == "error" || json["status"] == "fail" {
                let msg = json["message"].as_str().unwrap_or("Trade Delete Failed");
                println!(
                    "[WATCHER] Delete order {} - Error response: '{}' (retry_count: {}/2, retry_enabled: {})",
                    trade_id, msg, retry_count, retry_on_session_error
                );

                if retry_on_session_error
                    && (msg.contains("Invalid Login") || msg.contains("Invalid Server Time"))
                    && retry_count < 2
                {
                    // Smart check: verify if the order actually exists before re-logging
                    println!(
                        "[WATCHER] Delete order {} - Checking if order still exists before re-login...",
                        trade_id
                    );

                    match self.get_pending_orders().await {
                        Ok(pending_orders) => {
                            let order_exists = pending_orders.iter().any(|id| id == order_id);

                            if !order_exists {
                                println!(
                                    "[WATCHER] Delete order {} - Order not in pending list. Likely already deleted/executed.",
                                    trade_id
                                );
                                // Treat as success - order is gone
                                return Ok(json!({
                                    "status": "success",
                                    "message": "Order already processed",
                                    "note": "Order not found in pending orders"
                                }));
                            }

                            println!(
                                "[WATCHER] Delete order {} - Order still exists. Proceeding with re-login.",
                                trade_id
                            );
                        }
                        Err(e) => {
                            println!(
                                "[WATCHER] Delete order {} - Could not verify order existence: {}. Proceeding with re-login anyway.",
                                trade_id, e
                            );
                        }
                    }

                    retry_count += 1;
                    println!(
                        "[WATCHER] {} detected. Re-logging (Attempt {})...",
                        msg, retry_count
                    );
                    if let Err(e) = self.perform_login(true).await {
                        return Err(format!("Re-login failed during delete_order: {}", e));
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue; // Retry the loop
                }
                println!("[WATCHER] Delete order {} - Failed: {}", trade_id, msg);
                return Err(msg.to_string());
            }

            println!("[WATCHER] Delete order {} - Success!", trade_id);
            return Ok(json);
        }
    }

    pub async fn load_scripts(&self) -> Result<(), String> {
        let mut retry_count = 0;
        let url = "https://www.gmglobal.org/ajaxfiles/market_watch_list1.php";

        loop {
            println!("[GMGLOBAL] Loading script master list...");

            let http = self.client.read().await;
            let resp_result = http.post(url).json(&serde_json::json!({})).send().await;

            drop(http); // Release read lock

            let resp = resp_result.map_err(|e| e.to_string())?;
            let body = resp.text().await.map_err(|e| e.to_string())?;

            if body.contains("Invalid Login") || body.contains("Invalid Server Time") {
                if retry_count < 2 {
                    retry_count += 1;
                    println!("[GMGLOBAL] Session error detected during load_scripts. Re-logging (Attempt {})...", retry_count);
                    if let Err(e) = self.perform_login(true).await {
                        return Err(format!("Re-login failed during load_scripts: {}", e));
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue; // Retry the loop
                }
                return Err("Failed to load scripts even after re-login".to_string());
            }

            let parsed: MarketWatchResponse = serde_json::from_str(&body)
                .map_err(|e| format!("Failed to parse script list: {} | {}", e, body))?;

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
            return Ok(());
        }
    }

    pub async fn get_pending_orders(&self) -> Result<Vec<String>, String> {
        let mut retry_count = 0;
        let url = "https://www.gmglobal.org/datatables/order_book.php";

        loop {
            let uid = self.user_id.read().await.clone();
            println!("[WATCHER] Fetching pending orders (UserID: {})...", uid);

            // Constructing the body as form data since it's a POST with DataTables params
            let payload = format!(
                "sEcho=1&iColumns=13&sColumns=%2C%2C%2C%2C%2C%2C%2C%2C%2C%2C%2C%2C&iDisplayStart=0&iDisplayLength=100&mDataProp_0=0&sSearch_0=&bRegex_0=false&bSearchable_0=true&bSortable_0=false&mDataProp_1=1&sSearch_1=&bRegex_1=false&bSearchable_1=true&bSortable_1=true&mDataProp_2=2&sSearch_2=&bRegex_2=false&bSearchable_2=true&bSortable_2=true&mDataProp_3=3&sSearch_3=&bRegex_3=false&bSearchable_3=true&bSortable_3=true&mDataProp_4=4&sSearch_4=&bRegex_4=false&bSearchable_4=true&bSortable_4=true&mDataProp_5=5&sSearch_5=&bRegex_5=false&bSearchable_5=true&bSortable_5=true&mDataProp_6=6&sSearch_6=&bRegex_6=false&bSearchable_6=true&bSortable_6=true&mDataProp_7=7&sSearch_7=&bRegex_7=false&bSearchable_7=true&bSortable_7=true&mDataProp_8=8&sSearch_8=&bRegex_8=false&bSearchable_8=true&bSortable_8=true&mDataProp_9=9&sSearch_9=&bRegex_9=false&bSearchable_9=true&bSortable_9=true&mDataProp_10=10&sSearch_10=&bRegex_10=false&bSearchable_10=true&bSortable_10=true&mDataProp_11=11&sSearch_11=&bRegex_11=false&bSearchable_11=true&bSortable_11=false&mDataProp_12=12&sSearch_12=&bRegex_12=false&bSearchable_12=true&bSortable_12=false&sSearch=&bRegex=false&iSortCol_0=10&sSortDir_0=desc&iSortingCols=1&market_type_id=&script_id=&user_id={uid}&broker_id=&is_pending=&is_executed=&trade_type=&master_user_id=&start_end=&end_date=",
                uid = uid
            );

            let http = self.client.read().await;
            let resp_result = http
                .post(url)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(payload)
                .send()
                .await;

            drop(http);

            let resp = resp_result.map_err(|e| e.to_string())?;
            let body = resp.text().await.map_err(|e| e.to_string())?;

            if body.contains("Invalid Login") || body.contains("Invalid Server Time") {
                if retry_count < 2 {
                    retry_count += 1;
                    println!("[WATCHER] Session error during get_pending_orders. Re-logging...");
                    if let Err(e) = self.perform_login(true).await {
                        return Err(format!("Re-login failed: {}", e));
                    }
                    continue;
                }
                return Err("Failed after re-login".to_string());
            }

            let data_table_resp: crate::trade::DataTableResponse = match serde_json::from_str(&body)
            {
                Ok(v) => v,
                Err(e) => {
                    // Try to see if it's a success but empty or special case
                    println!(
                        "[WATCHER] Failed to parse order book response: {}. Body: {}",
                        e, body
                    );
                    return Err(format!("Parse error: {}", e));
                }
            };

            let mut order_ids = Vec::new();
            for row in data_table_resp.data {
                // Check if it's actually pending at index 9
                let status = row.get(9).and_then(|v| v.as_str()).unwrap_or("");
                if !status.to_lowercase().contains("pending") {
                    continue;
                }

                // According to provided example, ID is at index 13
                if row.len() > 13 {
                    let id_val = &row[13];
                    let id_str = crate::trade::value_to_u64(id_val).to_string();

                    if !id_str.is_empty() {
                        println!(
                            "[WATCHER] Found pending order: {} (Status: {})",
                            id_str, status
                        );
                        order_ids.push(id_str);
                    }
                }
            }

            return Ok(order_ids);
        }
    }

    pub async fn get_open_positions(&self) -> Result<Vec<serde_json::Value>, String> {
        let mut retry_count = 0;
        let url = "https://www.gmglobal.org/datatables/position_book_list.php";

        loop {
            let uid = self.user_id.read().await.clone();
            println!("[WATCHER] Fetching open positions (UserID: {})...", uid);

            let payload = format!(
                "sEcho=1&iColumns=13&sColumns=%2C%2C%2C%2C%2C%2C%2C%2C%2C%2C%2C%2C&iDisplayStart=0&iDisplayLength=-1&mDataProp_0=0&sSearch_0=&bRegex_0=false&bSearchable_0=true&bSortable_0=true&mDataProp_1=1&sSearch_1=&bRegex_1=false&bSearchable_1=true&bSortable_1=true&mDataProp_2=2&sSearch_2=&bRegex_2=false&bSearchable_2=true&bSortable_2=true&mDataProp_3=3&sSearch_3=&bRegex_3=false&bSearchable_3=true&bSortable_3=true&mDataProp_4=4&sSearch_4=&bRegex_4=false&bSearchable_4=true&bSortable_4=false&mDataProp_5=5&sSearch_5=&bRegex_5=false&bSearchable_5=true&bSortable_5=false&mDataProp_6=6&sSearch_6=&bRegex_6=false&bSearchable_6=true&bSortable_6=false&mDataProp_7=7&sSearch_7=&bRegex_7=false&bSearchable_7=true&bSortable_7=false&mDataProp_8=8&sSearch_8=&bRegex_8=false&bSearchable_8=true&bSortable_8=false&mDataProp_9=9&sSearch_9=&bRegex_9=false&bSearchable_9=true&bSortable_9=false&mDataProp_10=10&sSearch_10=&bRegex_10=false&bSearchable_10=true&bSortable_10=false&mDataProp_11=11&sSearch_11=&bRegex_11=false&bSearchable_11=true&bSortable_11=false&mDataProp_12=12&sSearch_12=&bRegex_12=false&bSearchable_12=true&bSortable_12=false&sSearch=&bRegex=false&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&broker_id=&master_user_id=&user_id={uid}&script_id=&market_type_id=&all_outstanding=0&group_by=u.user_full_name&expiry_date=",
                uid = uid
            );

            let http = self.client.read().await;
            let resp_result = http
                .post(url)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(payload)
                .send()
                .await;

            drop(http);

            let resp = resp_result.map_err(|e| e.to_string())?;
            let body = resp.text().await.map_err(|e| e.to_string())?;

            if body.contains("Invalid Login") || body.contains("Invalid Server Time") {
                if retry_count < 2 {
                    retry_count += 1;
                    println!("[WATCHER] Session error during get_open_positions. Re-logging...");
                    if let Err(e) = self.perform_login(true).await {
                        return Err(format!("Re-login failed: {}", e));
                    }
                    continue;
                }
                return Err("Failed after re-login".to_string());
            }

            let data_table_resp: crate::trade::DataTableResponse = match serde_json::from_str(&body)
            {
                Ok(v) => v,
                Err(e) => {
                    println!(
                        "[WATCHER] Failed to parse position book response: {}. Body: {}",
                        e, body
                    );
                    return Err(format!("Parse error: {}", e));
                }
            };

            let mut positions = Vec::new();
            for row in data_table_resp.data {
                if row.len() > 22 {
                    let product = row[0].as_str().unwrap_or("").to_string();
                    let net_qty = crate::trade::value_to_u64(&row[22]);

                    if net_qty == 0 {
                        continue;
                    }

                    // Parse buy/sell qty to determine side
                    let buy_qty_str = row[4].as_str().unwrap_or("0");
                    let sell_qty_str = row[6].as_str().unwrap_or("0");

                    let buy_qty = buy_qty_str
                        .split_whitespace()
                        .next()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    let sell_qty = sell_qty_str
                        .split_whitespace()
                        .next()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);

                    let side = if buy_qty > sell_qty { "buy" } else { "sell" };
                    let avg_price = if buy_qty > sell_qty {
                        crate::trade::value_to_f64(&row[5])
                    } else {
                        crate::trade::value_to_f64(&row[7])
                    };

                    // Extract script_id (index 24) and script_expiry_id (index 23)
                    let script_expiry_id =
                        row.get(23).and_then(|v| v.as_str()).map(|s| s.to_string());
                    let script_id = row.get(24).and_then(|v| v.as_str()).map(|s| s.to_string());

                    positions.push(serde_json::json!({
                        "InstrumentIdentifier": product,
                        "Quantity": net_qty,
                        "TradeSide": side,
                        "AveragePrice": avg_price,
                        "script_id": script_id,
                        "script_expiry_id": script_expiry_id
                    }));
                }
            }

            return Ok(positions);
        }
    }

    pub async fn has_script(&self, product: &str) -> bool {
        self.scripts.read().await.contains_key(product)
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

#[async_trait::async_trait]
impl crate::market_data::TradeExecutor for GMGlobalWatcher {
    async fn place_trade(
        &self,
        product: &str,
        price: Option<f64>,
        side: crate::trade::TradeSide,
        order_type: crate::trade::OrderType,
        trade_qty: u32,
        market_type_id: u8,
        script_id: Option<String>,
        script_expiry_id: Option<String>,
    ) -> Result<serde_json::Value, String> {
        self.place_trade(
            product,
            price,
            side,
            order_type,
            trade_qty,
            market_type_id,
            script_id,
            script_expiry_id,
        )
        .await
    }

    async fn delete_order(&self, order_id: &str) -> Result<serde_json::Value, String> {
        self.delete_order(order_id).await
    }

    async fn get_open_positions(&self) -> Result<Vec<serde_json::Value>, String> {
        self.get_open_positions().await
    }
}
