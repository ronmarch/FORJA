// ═══════════════════════════════════════════════════════
// FORJA v1.0 - Ingesta WebSocket
// ═══════════════════════════════════════════════════════
// Conecta con Binance WebSocket para recibir trades
// tick-by-tick en tiempo real.
//
// URLs de Binance:
//   Spot:    wss://stream.binance.com:9443/ws/{symbol}@trade
//   Futures: wss://fstream.binance.com/ws/{symbol}@trade
//
// El símbolo debe ir en minúsculas (ej: btcusdt)
// ═══════════════════════════════════════════════════════

use crate::normalizador::normalizador::{
    normalizar_futures, normalizar_spot, Mercado, TradeFORJA,
};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tracing::{error, info, warn};

// ───────────────────────────────────────────────────────
// Constantes
// ───────────────────────────────────────────────────────

/// URL base WebSocket Spot de Binance
const BINANCE_SPOT_WS: &str = "wss://stream.binance.com:9443/ws";

/// URL base WebSocket Futures USD-M de Binance
const BINANCE_FUTURES_WS: &str = "wss://fstream.binance.com/ws";

/// Tiempo de espera antes de reconectar (segundos)
const RECONEXION_ESPERA_SEG: u64 = 5;

/// Máximo de reintentos consecutivos antes de pausar más
const MAX_REINTENTOS_RAPIDOS: u32 = 5;

/// Pausa larga después de muchos reintentos (segundos)
const PAUSA_LARGA_SEG: u64 = 30;

// ───────────────────────────────────────────────────────
// Mensaje de trade procesado
// ───────────────────────────────────────────────────────

/// Estructura que envía la ingesta al resto del sistema.
/// Incluye el trade normalizado + metadata de origen.
#[derive(Debug, Clone)]
pub struct TradeEvento {
    /// Trade normalizado en formato FORJA
    pub trade: TradeFORJA,
    /// Mercado de origen (Spot o Futures)
    pub mercado: Mercado,
    /// Símbolo del par (ej: "BTCUSDT")
    pub simbolo: String,
}

// ───────────────────────────────────────────────────────
// Construir URL de WebSocket
// ───────────────────────────────────────────────────────

/// Genera la URL del WebSocket para el stream de trades
fn construir_url(simbolo: &str, mercado: Mercado) -> String {
    let base = match mercado {
        Mercado::Spot => BINANCE_SPOT_WS,
        Mercado::Futures => BINANCE_FUTURES_WS,
    };
    // Binance requiere el símbolo en minúsculas
    let simbolo_lower = simbolo.to_lowercase();
    format!("{}/{}@trade", base, simbolo_lower)
}

// ───────────────────────────────────────────────────────
// Conexión WebSocket con reconexión automática
// ───────────────────────────────────────────────────────

/// Inicia una conexión WebSocket a Binance para un par y mercado.
/// Reconecta automáticamente ante caídas.
/// Envía cada trade normalizado por el canal `tx`.
///
/// # Argumentos
/// * `simbolo` - Par de trading (ej: "BTCUSDT")
/// * `mercado` - Spot o Futures
/// * `tx` - Canal para enviar trades procesados
/// * `stop` - Señal para detener la conexión
pub async fn iniciar_stream(
    simbolo: String,
    mercado: Mercado,
    tx: mpsc::UnboundedSender<TradeEvento>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let url = construir_url(&simbolo, mercado);
    let mut reintentos: u32 = 0;
    let mut trades_recibidos: u64 = 0;

    info!(
        "INGESTA | {} {} | Iniciando stream → {}",
        mercado, simbolo, url
    );

    loop {
        // ── Verificar señal de stop ──
        if *stop.borrow() {
            info!(
                "INGESTA | {} {} | Stop recibido. Total trades: {}",
                mercado, simbolo, trades_recibidos
            );
            break;
        }

        // ── Conectar ──
        info!(
            "INGESTA | {} {} | Conectando... (intento {})",
            mercado,
            simbolo,
            reintentos + 1
        );

        let conexion = connect_async(&url).await;

        match conexion {
            Ok((ws_stream, _response)) => {
                info!("INGESTA | {} {} | Conectado ✓", mercado, simbolo);
                reintentos = 0; // Reset reintentos al conectar exitosamente

                let (_write, mut read) = ws_stream.split();

                // ── Leer mensajes ──
                loop {
                    tokio::select! {
                        // Mensaje del WebSocket
                        msg = read.next() => {
                            match msg {
                                Some(Ok(mensaje)) => {
                                    if mensaje.is_text() {
                                        let json_raw = mensaje.to_text().unwrap_or("");

                                        // Normalizar según mercado
                                        let resultado = match mercado {
                                            Mercado::Spot => normalizar_spot(json_raw),
                                            Mercado::Futures => normalizar_futures(json_raw),
                                        };

                                        match resultado {
                                            Ok(trade) => {
                                                trades_recibidos += 1;

                                                let evento = TradeEvento {
                                                    trade,
                                                    mercado,
                                                    simbolo: simbolo.clone(),
                                                };

                                                // Enviar al canal
                                                if tx.send(evento).is_err() {
                                                    error!(
                                                        "INGESTA | {} {} | Canal cerrado, deteniendo",
                                                        mercado, simbolo
                                                    );
                                                    return;
                                                }

                                                // Log cada 1000 trades
                                                if trades_recibidos % 1000 == 0 {
                                                    info!(
                                                        "INGESTA | {} {} | {} trades procesados",
                                                        mercado, simbolo, trades_recibidos
                                                    );
                                                }
                                            }
                                            Err(e) => {
                                                warn!(
                                                    "INGESTA | {} {} | Trade descartado: {}",
                                                    mercado, simbolo, e
                                                );
                                            }
                                        }
                                    }
                                    // Ignorar mensajes binarios (pings internos)
                                }
                                Some(Err(e)) => {
                                    error!(
                                        "INGESTA | {} {} | Error WebSocket: {}",
                                        mercado, simbolo, e
                                    );
                                    break; // Salir del loop de lectura → reconectar
                                }
                                None => {
                                    warn!(
                                        "INGESTA | {} {} | Stream cerrado por servidor",
                                        mercado, simbolo
                                    );
                                    break; // Reconectar
                                }
                            }
                        }
                        // Señal de stop
                        _ = stop.changed() => {
                            if *stop.borrow() {
                                info!(
                                    "INGESTA | {} {} | Stop recibido. Total trades: {}",
                                    mercado, simbolo, trades_recibidos
                                );
                                return;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                error!(
                    "INGESTA | {} {} | Error al conectar: {}",
                    mercado, simbolo, e
                );
            }
        }

        // ── Lógica de reconexión ──
        reintentos += 1;
        let espera = if reintentos >= MAX_REINTENTOS_RAPIDOS {
            warn!(
                "INGESTA | {} {} | Demasiados reintentos ({}), pausa larga de {}s",
                mercado, simbolo, reintentos, PAUSA_LARGA_SEG
            );
            reintentos = 0; // Reset para nuevo ciclo
            PAUSA_LARGA_SEG
        } else {
            RECONEXION_ESPERA_SEG
        };

        info!(
            "INGESTA | {} {} | Reconectando en {}s...",
            mercado, simbolo, espera
        );
        tokio::time::sleep(tokio::time::Duration::from_secs(espera)).await;
    }
}

// ───────────────────────────────────────────────────────
// Gestor de múltiples streams
// ───────────────────────────────────────────────────────

/// Configuración de un par a monitorear
#[derive(Debug, Clone)]
pub struct ConfigPar {
    /// Símbolo del par (ej: "BTCUSDT")
    pub simbolo: String,
    /// Si capturar trades spot
    pub spot: bool,
    /// Si capturar trades futures
    pub futures: bool,
}

impl ConfigPar {
    pub fn new(simbolo: &str, spot: bool, futures: bool) -> Self {
        Self {
            simbolo: simbolo.to_uppercase(),
            spot,
            futures,
        }
    }
}

/// Inicia todos los streams configurados.
/// Retorna el receptor de trades y el emisor de stop.
///
/// # Ejemplo
/// ```
/// let pares = vec![
///     ConfigPar::new("BTCUSDT", true, true),
///     ConfigPar::new("SOLUSDT", true, true),
/// ];
/// let (rx, stop_tx) = iniciar_ingesta(pares).await;
/// ```
pub async fn iniciar_ingesta(
    pares: Vec<ConfigPar>,
) -> (
    mpsc::UnboundedReceiver<TradeEvento>,
    tokio::sync::watch::Sender<bool>,
) {
    // Canal para trades (unbounded porque los trades llegan a alta velocidad)
    let (tx, rx) = mpsc::unbounded_channel::<TradeEvento>();

    // Canal para señal de stop
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);

    // Lanzar un task por cada stream
    for par in &pares {
        if par.spot {
            let tx_clone = tx.clone();
            let stop_clone = stop_rx.clone();
            let simbolo = par.simbolo.clone();

            tokio::spawn(async move {
                iniciar_stream(simbolo, Mercado::Spot, tx_clone, stop_clone).await;
            });
        }

        if par.futures {
            let tx_clone = tx.clone();
            let stop_clone = stop_rx.clone();
            let simbolo = par.simbolo.clone();

            tokio::spawn(async move {
                iniciar_stream(simbolo, Mercado::Futures, tx_clone, stop_clone).await;
            });
        }
    }

    let total_streams = pares.iter().fold(0, |acc, p| {
        acc + if p.spot { 1 } else { 0 } + if p.futures { 1 } else { 0 }
    });
    info!("INGESTA | {} streams iniciados", total_streams);

    (rx, stop_tx)
}

// ───────────────────────────────────────────────────────
// Tests
// ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_construir_url_spot() {
        let url = construir_url("BTCUSDT", Mercado::Spot);
        assert_eq!(url, "wss://stream.binance.com:9443/ws/btcusdt@trade");
    }

    #[test]
    fn test_construir_url_futures() {
        let url = construir_url("BTCUSDT", Mercado::Futures);
        assert_eq!(url, "wss://fstream.binance.com/ws/btcusdt@trade");
    }

    #[test]
    fn test_construir_url_minusculas() {
        let url = construir_url("SolUSDT", Mercado::Spot);
        assert_eq!(url, "wss://stream.binance.com:9443/ws/solusdt@trade");
    }

    #[test]
    fn test_config_par() {
        let par = ConfigPar::new("btcusdt", true, true);
        assert_eq!(par.simbolo, "BTCUSDT"); // Se convierte a mayúsculas
        assert!(par.spot);
        assert!(par.futures);
    }

    #[test]
    fn test_config_par_solo_spot() {
        let par = ConfigPar::new("SOLUSDT", true, false);
        assert!(par.spot);
        assert!(!par.futures);
    }
}
