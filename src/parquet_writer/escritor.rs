// ═══════════════════════════════════════════════════════
// FORJA v1.0 - Escritor Parquet + Scheduler
// ═══════════════════════════════════════════════════════
// Schema Parquet:
//   timestamp   u64  - ms UTC
//   price       f64
//   volume_usdt f64
//   volume_base f64
//   side        u8   - 0=BUY, 1=SELL
//   seg         u8   - segmento P1-P14 (1-14)
// ═══════════════════════════════════════════════════════

use crate::config::control::{
    SesionActual, SesionSnapshot, segundos_hasta_proximo_volcado,
    sesion_actual_utc, proximo_volcado_utc,
};
use crate::normalizador::normalizador::TradeFORJA;
use arrow::array::{Float64Array, UInt64Array, UInt8Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::collections::VecDeque;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

// ───────────────────────────────────────────────────────
// Trade con segmento (lo que se almacena en buffer Parquet)
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub struct TradeConSeg {
    pub trade: TradeFORJA,
    pub seg: u8,
}

// ───────────────────────────────────────────────────────
// Escritor de Parquet
// ───────────────────────────────────────────────────────

pub fn escribir_parquet(
    trades: &[TradeConSeg],
    ruta: &str,
) -> Result<usize, String> {
    if trades.is_empty() {
        return Ok(0);
    }

    if let Some(parent) = Path::new(ruta).parent() {
        fs::create_dir_all(parent).map_err(|e| format!("Error directorio: {}", e))?;
    }

    let len = trades.len();

    let timestamps: Vec<u64> = trades.iter().map(|t| t.trade.timestamp).collect();
    let prices: Vec<f64> = trades.iter().map(|t| t.trade.price).collect();
    let volumes_usdt: Vec<f64> = trades.iter().map(|t| t.trade.volume_usdt).collect();
    let volumes_base: Vec<f64> = trades.iter().map(|t| t.trade.volume_base).collect();
    let sides: Vec<u8> = trades.iter().map(|t| t.trade.side).collect();
    let segs: Vec<u8> = trades.iter().map(|t| t.seg).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::UInt64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("volume_usdt", DataType::Float64, false),
        Field::new("volume_base", DataType::Float64, false),
        Field::new("side", DataType::UInt8, false),
        Field::new("seg", DataType::UInt8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(timestamps)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(Float64Array::from(volumes_usdt)),
            Arc::new(Float64Array::from(volumes_base)),
            Arc::new(UInt8Array::from(sides)),
            Arc::new(UInt8Array::from(segs)),
        ],
    ).map_err(|e| format!("Error batch: {}", e))?;

    let file = fs::File::create(ruta).map_err(|e| format!("Error archivo: {}", e))?;
    let mut writer = ArrowWriter::try_new(file, schema, None)
        .map_err(|e| format!("Error writer: {}", e))?;
    writer.write(&batch).map_err(|e| format!("Error write: {}", e))?;
    writer.close().map_err(|e| format!("Error close: {}", e))?;

    tracing::info!("PARQUET | Escrito: {} ({} trades)", ruta, len);
    Ok(len)
}

pub fn generar_ruta(base: &str, par: &str, mercado: &str) -> String {
    let now = chrono::Utc::now();
    let fecha = now.format("%Y-%m-%d_%H-%M").to_string();
    format!("{}/{}/{}/{}.parquet", base, par, mercado, fecha)
}

// ───────────────────────────────────────────────────────
// Scheduler de volcado automático (8hrs)
// ───────────────────────────────────────────────────────

pub async fn iniciar_scheduler(
    buffer_spot: Arc<RwLock<VecDeque<TradeConSeg>>>,
    buffer_futures: Arc<RwLock<VecDeque<TradeConSeg>>>,
    sesion_anterior_state: Arc<RwLock<SesionSnapshot>>,
    sesion_actual_state: Arc<RwLock<SesionActual>>,
    par: String,
    base_dir: String,
) {
    tracing::info!(
        "SCHEDULER | Volcado cada 8hrs (04:00, 12:00, 20:00 UTC) | Próximo: {}",
        proximo_volcado_utc()
    );

    loop {
        let espera = segundos_hasta_proximo_volcado();
        tracing::info!(
            "SCHEDULER | Próximo volcado en {}h {}m | {}",
            espera / 3600, (espera % 3600) / 60, proximo_volcado_utc()
        );

        tokio::time::sleep(tokio::time::Duration::from_secs(espera)).await;

        // Snapshot sesión que cierra
        let snapshot = {
            let sesion = sesion_actual_state.read().await;
            sesion.generar_snapshot()
        };
        tracing::info!(
            "SCHEDULER | Sesión {} cerrada | O${:.2} C${:.2} | PD:{} {} | {} trades",
            snapshot.nombre, snapshot.precio_apertura, snapshot.precio_cierre,
            snapshot.pd_direccion, snapshot.segmento_dominante, snapshot.trades_total,
        );
        *sesion_anterior_state.write().await = snapshot;

        // Volcar Spot
        {
            let mut buf = buffer_spot.write().await;
            if !buf.is_empty() {
                let trades: Vec<TradeConSeg> = buf.drain(..).collect();
                let ruta = generar_ruta(&base_dir, &par, "spot");
                match escribir_parquet(&trades, &ruta) {
                    Ok(n) => tracing::info!("SCHEDULER | SPOT volcado: {} trades", n),
                    Err(e) => tracing::error!("SCHEDULER | SPOT error: {}", e),
                }
            }
        }

        // Volcar Futures
        {
            let mut buf = buffer_futures.write().await;
            if !buf.is_empty() {
                let trades: Vec<TradeConSeg> = buf.drain(..).collect();
                let ruta = generar_ruta(&base_dir, &par, "futures");
                match escribir_parquet(&trades, &ruta) {
                    Ok(n) => tracing::info!("SCHEDULER | FUTURES volcado: {} trades", n),
                    Err(e) => tracing::error!("SCHEDULER | FUTURES error: {}", e),
                }
            }
        }

        // Nueva sesión
        let (_idx, nombre) = sesion_actual_utc();
        *sesion_actual_state.write().await = SesionActual::nueva(nombre);
        tracing::info!("SCHEDULER | Nueva sesión: {}", nombre);
    }
}
