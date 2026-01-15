use serde::Serialize;

// =======================
// Trade enums
// =======================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TradeSide {
    Sell = 1,
    Buy = 0,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OrderType {
    Market = 0,
    #[allow(dead_code)]
    Limit = 1,
    StopLoss = 2,
}

// =======================
// Trade request payload
// =======================

#[derive(Debug, Serialize)]
pub struct TradeRequest {
    // Known fixed fields
    pub market_type_id: u8, // always 2
    pub trade_type: u8,     // 0=BUY, 1=SELL
    pub trade_type_x: u8,   // 1=LIMIT, 2=MARKET, 3=SL

    #[serde(skip_serializing_if = "Option::is_none")]
    pub trade_rate: Option<String>,

    pub trade_qty: u32, // usually 1
    pub trade_lot: u8,  // usually 1
    pub check_script_name: String,
    pub user_id: String, // empty string works
    pub device_type: u8, // always 0

    // Unknown / optional (omit if None)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub script_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub script_expiry_id: Option<String>,
}

// =======================
// Helper constructor
// =======================

impl TradeRequest {
    pub fn new(
        product: &str,
        price: Option<f64>,
        side: TradeSide,
        order_type: OrderType,
        trade_qty: u32,
        market_type_id: u8,
    ) -> Self {
        let trade_rate: Option<String> = price.map(|p| p.to_string());
        Self {
            market_type_id,
            trade_type: side as u8,
            trade_type_x: order_type as u8,
            trade_rate: trade_rate,
            trade_qty: trade_qty,
            trade_lot: 1,
            check_script_name: product.to_string(),
            user_id: "".to_string(),
            device_type: 0,
            script_id: None,
            script_expiry_id: None,
        }
    }
}

// =======================
// DataTable response
// =======================

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
pub struct DataTableResponse {
    #[serde(rename = "sEcho")]
    pub s_echo: serde_json::Value,
    #[serde(rename = "iTotalRecords")]
    pub total_records: serde_json::Value,
    #[serde(rename = "iTotalDisplayRecords")]
    pub total_display_records: serde_json::Value,
    #[serde(rename = "aaData")]
    pub data: Vec<Vec<serde_json::Value>>,
}
pub fn value_to_f64(v: &serde_json::Value) -> f64 {
    if let Some(f) = v.as_f64() {
        f
    } else if let Some(i) = v.as_i64() {
        i as f64
    } else if let Some(s) = v.as_str() {
        s.parse::<f64>().unwrap_or(0.0)
    } else {
        0.0
    }
}

pub fn value_to_u64(v: &serde_json::Value) -> u64 {
    if let Some(u) = v.as_u64() {
        u
    } else if let Some(i) = v.as_i64() {
        i.unsigned_abs()
    } else if let Some(s) = v.as_str() {
        s.parse::<i64>().map(|i| i.unsigned_abs()).unwrap_or(0)
    } else {
        0
    }
}
