use serde::Serialize;

// =======================
// Trade enums
// =======================

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum TradeSide {
    Sell = 1,
    Buy = 0,
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum OrderType {
    Limit = 2,
    Market = 0,
    StopLoss = 1,
}

// =======================
// Trade request payload
// =======================

#[derive(Debug, Serialize)]
pub struct TradeRequest {
    // Known fixed fields
    pub market_type_id: u8,     // always 2
    pub trade_type: u8,          // 0=BUY, 1=SELL
    pub trade_type_x: u8,        // 1=LIMIT, 2=MARKET, 3=SL

    #[serde(skip_serializing_if = "Option::is_none")]
    pub trade_rate: Option<String>,

    pub trade_qty: u32,          // usually 1
    pub trade_lot: u8,           // usually 1
    pub check_script_name: String,
    pub user_id: String,         // empty string works
    pub device_type: u8,         // always 0

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
    ) -> Self {
        let trade_rate: Option<String> = price.map(|p| p.to_string());
        // let trade_rate = match order_type {
        //     OrderType::Market => price.map(|p| p.to_string()),
        //     _ => price.map(|p| p.to_string()),
        // };
        Self {
            market_type_id: 2,
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
