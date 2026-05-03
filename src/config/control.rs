// ═══════════════════════════════════════════════════════
// FORJA v1.0 - Control del Sistema y Sesiones
// ═══════════════════════════════════════════════════════

use chrono::Timelike;
use serde::Serialize;
use crate::segmentacion::segmentacion::{NOMBRES_CORTOS, TOTAL_SEGMENTOS};
use crate::normalizador::normalizador::{TradeFORJA, SIDE_BUY};

// ───────────────────────────────────────────────────────
// Pares favoritos
// ───────────────────────────────────────────────────────

pub const PARES_FAVORITOS: [&str; 7] = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "WLDUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT",
];

// ───────────────────────────────────────────────────────
// Sesiones de mercado (bloques de 8 horas UTC)
// ───────────────────────────────────────────────────────

pub const HORAS_VOLCADO: [u32; 3] = [4, 12, 20];
pub const NOMBRES_SESION: [&str; 3] = ["Asia", "Londres", "NY"];

pub fn sesion_actual_utc() -> (usize, &'static str) {
    let h = chrono::Utc::now().hour();
    match h {
        20..=23 | 0..=3 => (0, "Asia"),
        4..=11 => (1, "Londres"),
        12..=19 => (2, "NY"),
        _ => (0, "Asia"),
    }
}

pub fn segundos_hasta_proximo_volcado() -> u64 {
    let now = chrono::Utc::now();
    let ahora_seg = (now.hour() * 3600 + now.minute() * 60 + now.second()) as u64;
    for &hora in &HORAS_VOLCADO {
        let volcado_seg = (hora * 3600) as u64;
        if volcado_seg > ahora_seg {
            return volcado_seg - ahora_seg;
        }
    }
    (24 * 3600) - ahora_seg + (HORAS_VOLCADO[0] * 3600) as u64
}

pub fn proximo_volcado_utc() -> String {
    let h = chrono::Utc::now().hour();
    for &hora in &HORAS_VOLCADO {
        if hora > h { return format!("{:02}:00 UTC", hora); }
    }
    format!("{:02}:00 UTC (+1d)", HORAS_VOLCADO[0])
}

pub fn minutos_hasta_proximo_volcado() -> u64 {
    segundos_hasta_proximo_volcado() / 60
}

// ───────────────────────────────────────────────────────
// Comandos del sistema (UI → Core)
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum ComandoSistema {
    /// Cambiar de par: programar volcado y cambio
    CambiarPar { nuevo_par: String },
    /// Stop programado: volcar y detener
    StopProgramado,
}

// ───────────────────────────────────────────────────────
// Estado de cambio pendiente
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Default)]
pub struct CambioPendiente {
    pub activo: bool,
    pub nuevo_par: String,
    pub minutos_restantes: u64,
    pub hora_volcado: String,
    pub es_stop: bool,
}

// ───────────────────────────────────────────────────────
// Snapshot de sesión (datos de cierre)
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Default)]
pub struct SesionSnapshot {
    pub nombre: String,
    pub precio_apertura: f64,
    pub precio_cierre: f64,
    pub segmento_dominante: String,
    pub pd_direccion: String,
    pub volumen_total: f64,
    pub buy_pct: f64,
    pub sell_pct: f64,
    pub trades_total: u64,
    pub valido: bool,
}

// ───────────────────────────────────────────────────────
// Sesión actual (en vivo)
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct SesionActual {
    pub nombre: String,
    pub precio_apertura: f64,
    pub precio_actual: f64,
    pub segmento_dominante: String,
    pub pd_direccion: String,
    pub volumen_total: f64,
    pub buy_pct: f64,
    pub sell_pct: f64,
    pub trades_total: u64,
    vol_por_segmento: [f64; TOTAL_SEGMENTOS],
    buy_por_segmento: [f64; TOTAL_SEGMENTOS],
    vol_buy_total: f64,
    vol_sell_total: f64,
    primer_trade: bool,
}

impl SesionActual {
    pub fn nueva(nombre: &str) -> Self {
        Self {
            nombre: nombre.to_string(),
            precio_apertura: 0.0, precio_actual: 0.0,
            segmento_dominante: "--".to_string(),
            pd_direccion: "--".to_string(),
            volumen_total: 0.0, buy_pct: 0.0, sell_pct: 0.0,
            trades_total: 0,
            vol_por_segmento: [0.0; TOTAL_SEGMENTOS],
            buy_por_segmento: [0.0; TOTAL_SEGMENTOS],
            vol_buy_total: 0.0, vol_sell_total: 0.0,
            primer_trade: true,
        }
    }

    pub fn procesar_trade(&mut self, trade: &TradeFORJA, segment_id: u8) {
        if self.primer_trade {
            self.precio_apertura = trade.price;
            self.primer_trade = false;
        }
        self.precio_actual = trade.price;
        self.trades_total += 1;
        self.volumen_total += trade.volume_usdt;

        if trade.side == SIDE_BUY {
            self.vol_buy_total += trade.volume_usdt;
        } else {
            self.vol_sell_total += trade.volume_usdt;
        }

        let idx = (segment_id as usize).saturating_sub(1);
        if idx < TOTAL_SEGMENTOS {
            self.vol_por_segmento[idx] += trade.volume_usdt;
            if trade.side == SIDE_BUY {
                self.buy_por_segmento[idx] += trade.volume_usdt;
            }
        }

        if self.volumen_total > 0.0 {
            self.buy_pct = (self.vol_buy_total / self.volumen_total) * 100.0;
            self.sell_pct = (self.vol_sell_total / self.volumen_total) * 100.0;
        }
        self.calcular_dominante();
    }

    fn calcular_dominante(&mut self) {
        let mut max_vol = 0.0f64;
        let mut max_idx = 0usize;
        for i in 0..TOTAL_SEGMENTOS {
            if self.vol_por_segmento[i] > max_vol {
                max_vol = self.vol_por_segmento[i];
                max_idx = i;
            }
        }
        if max_vol > 0.0 {
            self.segmento_dominante = NOMBRES_CORTOS[max_idx].to_string();
            let buy_pct_seg = (self.buy_por_segmento[max_idx] / max_vol) * 100.0;
            self.pd_direccion = if buy_pct_seg >= 50.0 { "Buy".to_string() } else { "Sell".to_string() };
        }
    }

    pub fn generar_snapshot(&self) -> SesionSnapshot {
        SesionSnapshot {
            nombre: self.nombre.clone(),
            precio_apertura: self.precio_apertura,
            precio_cierre: self.precio_actual,
            segmento_dominante: self.segmento_dominante.clone(),
            pd_direccion: self.pd_direccion.clone(),
            volumen_total: self.volumen_total,
            buy_pct: self.buy_pct, sell_pct: self.sell_pct,
            trades_total: self.trades_total,
            valido: self.trades_total > 0,
        }
    }
}
