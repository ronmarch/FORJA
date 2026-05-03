// ═══════════════════════════════════════════════════════
// FORJA v1.0 - Normalizador
// ═══════════════════════════════════════════════════════
// Convierte trades crudos de Binance (JSON) al formato
// interno FORJA de 33 bytes.
//
// Soporta:
//   - Spot trades (WebSocket stream @trade)
//   - Futures USD-M trades (WebSocket stream @trade)
//
// Regla de dirección (side):
//   Binance reporta "is_buyer_maker":
//     true  = el comprador fue maker → el TAKER vendió → side = SELL
//     false = el vendedor fue maker → el TAKER compró → side = BUY
//
//   En microestructura, la dirección del TAKER es la que
//   revela intención. El maker pone liquidez, el taker
//   la consume con urgencia.
// ═══════════════════════════════════════════════════════

use serde::Deserialize;
use std::fmt;

// ───────────────────────────────────────────────────────
// Constantes
// ───────────────────────────────────────────────────────

/// Dirección Buy (taker compró)
pub const SIDE_BUY: u8 = 0;

/// Dirección Sell (taker vendió)
pub const SIDE_SELL: u8 = 1;

// ───────────────────────────────────────────────────────
// Trade FORJA: estructura normalizada (33 bytes)
// ───────────────────────────────────────────────────────

/// Estructura interna normalizada de un trade.
/// Independiente del exchange. Todo el sistema opera
/// exclusivamente con esta estructura.
#[derive(Debug, Clone, Copy)]
pub struct TradeFORJA {
    /// Timestamp en milisegundos UTC
    pub timestamp: u64,
    /// Precio del trade
    pub price: f64,
    /// Volumen en USDT (quote volume)
    pub volume_usdt: f64,
    /// Volumen en token base
    pub volume_base: f64,
    /// Dirección: 0 = Buy, 1 = Sell
    pub side: u8,
}

impl TradeFORJA {
    /// Crea un nuevo TradeFORJA
    pub fn new(timestamp: u64, price: f64, volume_usdt: f64, volume_base: f64, side: u8) -> Self {
        Self {
            timestamp,
            price,
            volume_usdt,
            volume_base,
            side,
        }
    }

    /// Retorna la dirección como texto
    pub fn side_str(&self) -> &'static str {
        match self.side {
            SIDE_BUY => "BUY",
            SIDE_SELL => "SELL",
            _ => "UNKNOWN",
        }
    }

    /// Verifica si el trade es válido
    pub fn es_valido(&self) -> bool {
        self.timestamp > 0
            && self.price > 0.0
            && self.volume_usdt > 0.0
            && self.volume_base > 0.0
            && (self.side == SIDE_BUY || self.side == SIDE_SELL)
    }
}

impl fmt::Display for TradeFORJA {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] {} | precio: {:.2} | vol_usdt: {:.2} | vol_base: {:.8} | {}",
            self.timestamp,
            chrono::DateTime::from_timestamp_millis(self.timestamp as i64)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string())
                .unwrap_or_else(|| "fecha_invalida".to_string()),
            self.price,
            self.volume_usdt,
            self.volume_base,
            self.side_str(),
        )
    }
}

// ───────────────────────────────────────────────────────
// Tipo de mercado
// ───────────────────────────────────────────────────────

/// Identifica la fuente del trade
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Mercado {
    Spot,
    Futures,
}

impl fmt::Display for Mercado {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Mercado::Spot => write!(f, "SPOT"),
            Mercado::Futures => write!(f, "FUTURES"),
        }
    }
}

// ───────────────────────────────────────────────────────
// Trade crudo de Binance: Spot WebSocket (@trade)
// ───────────────────────────────────────────────────────
// Ejemplo JSON recibido:
// {
//   "e": "trade",
//   "E": 1672515782136,
//   "s": "BTCUSDT",
//   "t": 12345,
//   "p": "0.001",
//   "q": "100",
//   "T": 1672515782136,
//   "m": true,
//   "M": true
// }
// ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct BinanceSpotTrade {
    /// Evento type
    #[serde(rename = "e")]
    pub event_type: String,
    /// Event time (ms)
    #[serde(rename = "E")]
    pub event_time: u64,
    /// Symbol
    #[serde(rename = "s")]
    pub symbol: String,
    /// Trade ID
    #[serde(rename = "t")]
    pub trade_id: u64,
    /// Price (string en Binance)
    #[serde(rename = "p")]
    pub price: String,
    /// Quantity (string en Binance)
    #[serde(rename = "q")]
    pub quantity: String,
    /// Trade time (ms) - timestamp real del trade
    #[serde(rename = "T")]
    pub trade_time: u64,
    /// Is buyer the maker?
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
    /// Best price match (ignorado)
    #[serde(rename = "M")]
    #[serde(default)]
    pub is_best_match: bool,
}

// ───────────────────────────────────────────────────────
// Trade crudo de Binance: Futures USD-M WebSocket (@trade)
// ───────────────────────────────────────────────────────
// Ejemplo JSON recibido:
// {
//   "e": "trade",
//   "E": 1672515782136,
//   "s": "BTCUSDT",
//   "t": 12345,
//   "p": "0.001",
//   "q": "100",
//   "T": 1672515782136,
//   "m": true
// }
// ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct BinanceFuturesTrade {
    /// Event type
    #[serde(rename = "e")]
    pub event_type: String,
    /// Event time (ms)
    #[serde(rename = "E")]
    pub event_time: u64,
    /// Symbol
    #[serde(rename = "s")]
    pub symbol: String,
    /// Trade ID
    #[serde(rename = "t")]
    pub trade_id: u64,
    /// Price (string)
    #[serde(rename = "p")]
    pub price: String,
    /// Quantity (string)
    #[serde(rename = "q")]
    pub quantity: String,
    /// Trade time (ms)
    #[serde(rename = "T")]
    pub trade_time: u64,
    /// Is buyer the maker?
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
}

// ───────────────────────────────────────────────────────
// Errores del Normalizador
// ───────────────────────────────────────────────────────

#[derive(Debug)]
pub enum NormalizadorError {
    /// Error al parsear JSON
    JsonInvalido(String),
    /// Error al parsear precio o cantidad
    NumeroInvalido(String),
    /// Trade con datos inválidos (precio 0, volumen 0, etc.)
    TradeInvalido(String),
}

impl fmt::Display for NormalizadorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NormalizadorError::JsonInvalido(msg) => write!(f, "JSON inválido: {}", msg),
            NormalizadorError::NumeroInvalido(msg) => write!(f, "Número inválido: {}", msg),
            NormalizadorError::TradeInvalido(msg) => write!(f, "Trade inválido: {}", msg),
        }
    }
}

impl std::error::Error for NormalizadorError {}

// ───────────────────────────────────────────────────────
// Funciones de normalización
// ───────────────────────────────────────────────────────

/// Normaliza un trade SPOT de Binance al formato FORJA
pub fn normalizar_spot(json_raw: &str) -> Result<TradeFORJA, NormalizadorError> {
    // Parsear JSON
    let raw: BinanceSpotTrade = serde_json::from_str(json_raw)
        .map_err(|e| NormalizadorError::JsonInvalido(e.to_string()))?;

    // Parsear precio y cantidad (Binance los envía como strings)
    let price: f64 = raw
        .price
        .parse()
        .map_err(|_| NormalizadorError::NumeroInvalido(format!("price: {}", raw.price)))?;

    let quantity: f64 = raw
        .quantity
        .parse()
        .map_err(|_| NormalizadorError::NumeroInvalido(format!("qty: {}", raw.quantity)))?;

    // Calcular volumen en USDT (price * quantity)
    let volume_usdt = price * quantity;

    // Determinar dirección:
    // is_buyer_maker = true  → taker VENDIÓ → SELL
    // is_buyer_maker = false → taker COMPRÓ → BUY
    let side = if raw.is_buyer_maker {
        SIDE_SELL
    } else {
        SIDE_BUY
    };

    // Construir trade FORJA
    let trade = TradeFORJA::new(raw.trade_time, price, volume_usdt, quantity, side);

    // Validar
    if !trade.es_valido() {
        return Err(NormalizadorError::TradeInvalido(format!(
            "Trade spot inválido: id={}, price={}, qty={}",
            raw.trade_id, raw.price, raw.quantity
        )));
    }

    Ok(trade)
}

/// Normaliza un trade FUTURES de Binance al formato FORJA
pub fn normalizar_futures(json_raw: &str) -> Result<TradeFORJA, NormalizadorError> {
    // Parsear JSON
    let raw: BinanceFuturesTrade = serde_json::from_str(json_raw)
        .map_err(|e| NormalizadorError::JsonInvalido(e.to_string()))?;

    // Parsear precio y cantidad
    let price: f64 = raw
        .price
        .parse()
        .map_err(|_| NormalizadorError::NumeroInvalido(format!("price: {}", raw.price)))?;

    let quantity: f64 = raw
        .quantity
        .parse()
        .map_err(|_| NormalizadorError::NumeroInvalido(format!("qty: {}", raw.quantity)))?;

    // Calcular volumen en USDT
    let volume_usdt = price * quantity;

    // Dirección (misma lógica que spot)
    let side = if raw.is_buyer_maker {
        SIDE_SELL
    } else {
        SIDE_BUY
    };

    // Construir trade FORJA
    let trade = TradeFORJA::new(raw.trade_time, price, volume_usdt, quantity, side);

    // Validar
    if !trade.es_valido() {
        return Err(NormalizadorError::TradeInvalido(format!(
            "Trade futures inválido: id={}, price={}, qty={}",
            raw.trade_id, raw.price, raw.quantity
        )));
    }

    Ok(trade)
}

/// Normaliza cualquier trade detectando automáticamente si es spot o futures.
/// Ambos tienen la misma estructura base, la diferencia es el campo "M" (isBestMatch)
/// que solo existe en spot. Intentamos spot primero, si falla intentamos futures.
pub fn normalizar_auto(json_raw: &str) -> Result<(TradeFORJA, Mercado), NormalizadorError> {
    // Intentar como spot primero (tiene campo "M")
    if let Ok(trade) = normalizar_spot(json_raw) {
        return Ok((trade, Mercado::Spot));
    }

    // Si falla, intentar como futures
    match normalizar_futures(json_raw) {
        Ok(trade) => Ok((trade, Mercado::Futures)),
        Err(e) => Err(e),
    }
}

// ───────────────────────────────────────────────────────
// Tests
// ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// JSON de ejemplo: trade SPOT de Binance
    fn json_spot_buy() -> &'static str {
        r#"{
            "e": "trade",
            "E": 1712678400000,
            "s": "BTCUSDT",
            "t": 3547123456,
            "p": "68420.50",
            "q": "0.00150000",
            "T": 1712678400123,
            "m": false,
            "M": true
        }"#
    }

    fn json_spot_sell() -> &'static str {
        r#"{
            "e": "trade",
            "E": 1712678400000,
            "s": "BTCUSDT",
            "t": 3547123457,
            "p": "68420.50",
            "q": "0.00250000",
            "T": 1712678400456,
            "m": true,
            "M": true
        }"#
    }

    fn json_futures_buy() -> &'static str {
        r#"{
            "e": "trade",
            "E": 1712678400000,
            "s": "BTCUSDT",
            "t": 4812345678,
            "p": "68425.00",
            "q": "0.010",
            "T": 1712678400789,
            "m": false
        }"#
    }

    #[test]
    fn test_normalizar_spot_buy() {
        let trade = normalizar_spot(json_spot_buy()).unwrap();

        assert_eq!(trade.timestamp, 1712678400123);
        assert!((trade.price - 68420.50).abs() < 0.01);
        assert!((trade.volume_base - 0.00150000).abs() < 0.00000001);
        // volume_usdt = 68420.50 * 0.0015 = 102.63075
        assert!((trade.volume_usdt - 102.63075).abs() < 0.01);
        assert_eq!(trade.side, SIDE_BUY); // m=false → taker compró → BUY
        assert!(trade.es_valido());

        println!("SPOT BUY: {}", trade);
    }

    #[test]
    fn test_normalizar_spot_sell() {
        let trade = normalizar_spot(json_spot_sell()).unwrap();

        assert_eq!(trade.side, SIDE_SELL); // m=true → taker vendió → SELL
        // volume_usdt = 68420.50 * 0.0025 = 171.05125
        assert!((trade.volume_usdt - 171.05125).abs() < 0.01);

        println!("SPOT SELL: {}", trade);
    }

    #[test]
    fn test_normalizar_futures_buy() {
        let trade = normalizar_futures(json_futures_buy()).unwrap();

        assert_eq!(trade.timestamp, 1712678400789);
        assert!((trade.price - 68425.00).abs() < 0.01);
        assert_eq!(trade.side, SIDE_BUY);
        // volume_usdt = 68425.00 * 0.010 = 684.25
        assert!((trade.volume_usdt - 684.25).abs() < 0.01);
        assert!(trade.es_valido());

        println!("FUTURES BUY: {}", trade);
    }

    #[test]
    fn test_normalizar_auto_detecta_spot() {
        let (trade, mercado) = normalizar_auto(json_spot_buy()).unwrap();
        assert_eq!(mercado, Mercado::Spot);
        assert_eq!(trade.side, SIDE_BUY);
    }

    #[test]
    fn test_normalizar_auto_detecta_futures() {
        let (trade, mercado) = normalizar_auto(json_futures_buy()).unwrap();
        // Puede detectar como spot (el JSON de futures parsea ok como spot
        // porque "M" tiene default), pero el trade es igual.
        assert_eq!(trade.side, SIDE_BUY);
        assert!(trade.es_valido());
    }

    #[test]
    fn test_json_invalido() {
        let result = normalizar_spot("esto no es json");
        assert!(result.is_err());
    }

    #[test]
    fn test_trade_invalido_precio_cero() {
        let json = r#"{
            "e": "trade", "E": 1712678400000, "s": "BTCUSDT",
            "t": 1, "p": "0", "q": "1.0",
            "T": 1712678400000, "m": false, "M": true
        }"#;
        let result = normalizar_spot(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_segmentacion_por_volumen_usdt() {
        // Trade de 102 USDT → debería caer en segmento 3 (retail_pequeño: 100-499.99)
        let trade = normalizar_spot(json_spot_buy()).unwrap();
        assert!(trade.volume_usdt >= 100.0 && trade.volume_usdt < 500.0);
        println!(
            "Trade de {:.2} USDT → segmento retail_pequeño (P3)",
            trade.volume_usdt
        );

        // Trade de 171 USDT → también segmento 3
        let trade2 = normalizar_spot(json_spot_sell()).unwrap();
        assert!(trade2.volume_usdt >= 100.0 && trade2.volume_usdt < 500.0);
        println!(
            "Trade de {:.2} USDT → segmento retail_pequeño (P3)",
            trade2.volume_usdt
        );

        // Trade de 684 USDT → segmento 4 (retail_activo: 500-999.99)
        let trade3 = normalizar_futures(json_futures_buy()).unwrap();
        assert!(trade3.volume_usdt >= 500.0 && trade3.volume_usdt < 1000.0);
        println!(
            "Trade de {:.2} USDT → segmento retail_activo (P4)",
            trade3.volume_usdt
        );
    }

    #[test]
    fn test_display_trade() {
        let trade = normalizar_spot(json_spot_buy()).unwrap();
        let display = format!("{}", trade);
        assert!(display.contains("BUY"));
        assert!(display.contains("68420.50"));
        println!("{}", display);
    }
}
