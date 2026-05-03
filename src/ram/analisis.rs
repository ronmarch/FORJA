// ═══════════════════════════════════════════════════════
// FORJA v1.0 - Análisis en RAM
// ═══════════════════════════════════════════════════════
// Análisis en tiempo real sobre el buffer de 24h.
// Sin Parquet, sin IO — puro cálculo sobre VecDeque.
//
// Módulos:
//   S/R por volumen y trades (ventanas 1-8h)
//   RSI / MACD / VWAP por grupo de segmentos
//   CVD acumulado por ventana
//   Divergencia P1-P3 vs P7-P9
//   VWAP por sesión (Asia/Londres/NY)
// ═══════════════════════════════════════════════════════

use crate::normalizador::normalizador::{SIDE_BUY};
use crate::parquet_writer::escritor::TradeConSeg;
use serde::Serialize;
use std::collections::BTreeMap;

// ───────────────────────────────────────────────────────
// Constantes de segmentación (igual que sr.rs)
// ───────────────────────────────────────────────────────


// ───────────────────────────────────────────────────────
// Ventanas temporales
// ───────────────────────────────────────────────────────
pub const VENTANAS_HORAS: &[u64] = &[1, 2, 3, 4, 6, 8];

fn corte_ms(horas: u64) -> u64 {
    let ahora = chrono::Utc::now().timestamp_millis() as u64;
    ahora.saturating_sub(horas * 3600 * 1000)
}

// ───────────────────────────────────────────────────────
// S/R en RAM
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Default)]
pub struct NivelSRRam {
    pub precio: f64,
    pub vol_usdt: f64,
    pub vol_buy: f64,
    pub vol_sell: f64,
    pub trades: u64,
    pub buy_pct: f64,
    pub sell_pct: f64,
    pub seg_dominante: u8,
    pub score: f64,
    pub rank: u8,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct SRVentana {
    pub horas: u64,
    pub precio_ref: f64,
    pub bucket_size: f64,
    pub soportes_vol: Vec<NivelSRRam>,
    pub soportes_trades: Vec<NivelSRRam>,
    pub resistencias_vol: Vec<NivelSRRam>,
    pub resistencias_trades: Vec<NivelSRRam>,
}

#[derive(Debug, Clone, Default)]
struct BucketSR {
    precio_centro: f64,
    vol_total: f64,
    vol_buy: f64,
    vol_sell: f64,
    trades: u64,
    vol_seg: [f64; 14],
}

impl BucketSR {
    fn seg_dom(&self) -> u8 {
        self.vol_seg.iter().enumerate()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(i, _)| (i + 1) as u8)
            .unwrap_or(1)
    }
    fn buy_pct(&self) -> f64 {
        if self.vol_total > 0.0 { self.vol_buy / self.vol_total * 100.0 } else { 0.0 }
    }
    fn sell_pct(&self) -> f64 {
        if self.vol_total > 0.0 { self.vol_sell / self.vol_total * 100.0 } else { 0.0 }
    }
}

pub fn analizar_sr<'a>(
    trades: impl Iterator<Item = &'a TradeConSeg>,
    horas: u64,
) -> SRVentana {
    const BUCKET_PCT: f64 = 0.006;
    let corte = corte_ms(horas);

    let trades_filtrados: Vec<&TradeConSeg> = trades
        .filter(|t| t.trade.timestamp >= corte)
        .collect();

    if trades_filtrados.is_empty() {
        return SRVentana { horas, ..Default::default() };
    }

    // Precio de referencia = último trade
    let precio_ref = trades_filtrados.last().map(|t| t.trade.price).unwrap_or(0.0);
    if precio_ref <= 0.0 {
        return SRVentana { horas, precio_ref, ..Default::default() };
    }

    let bucket_size = precio_ref * BUCKET_PCT;
    let mut buckets: BTreeMap<i64, BucketSR> = BTreeMap::new();

    for t in &trades_filtrados {
        let idx = (t.trade.price / bucket_size).floor() as i64;
        let e = buckets.entry(idx).or_insert_with(|| BucketSR {
            precio_centro: (idx as f64 + 0.5) * bucket_size,
            ..Default::default()
        });
        e.vol_total += t.trade.volume_usdt;
        e.trades    += 1;
        let seg = t.seg as usize;
        e.vol_seg[seg.saturating_sub(1).min(13)] += t.trade.volume_usdt;
        if t.trade.side == SIDE_BUY { e.vol_buy += t.trade.volume_usdt; }
        else                  { e.vol_sell += t.trade.volume_usdt; }
    }

    let bucket_ref = (precio_ref / bucket_size).floor() as i64;

    let mut soportes: Vec<(i64, BucketSR)> = buckets.iter()
        .filter(|(&k, _)| k < bucket_ref)
        .map(|(&k, b)| (k, b.clone()))
        .collect();

    let mut resistencias: Vec<(i64, BucketSR)> = buckets.iter()
        .filter(|(&k, _)| k > bucket_ref)
        .map(|(&k, b)| (k, b.clone()))
        .collect();

    let top3 = |buckets: &[(i64, BucketSR)], _tipo: &str, es_sop: bool| -> Vec<NivelSRRam> {
        buckets.iter().take(3).enumerate().map(|(i, (_, b))| {
            let (score, vol_usdt) = if es_sop {
                (b.vol_buy, b.vol_buy)
            } else {
                (b.vol_sell, b.vol_sell)
            };
            NivelSRRam {
                precio: b.precio_centro,
                vol_usdt,
                vol_buy: b.vol_buy,
                vol_sell: b.vol_sell,
                trades: b.trades,
                buy_pct: b.buy_pct(),
                sell_pct: b.sell_pct(),
                seg_dominante: b.seg_dom(),
                score,
                rank: (i + 1) as u8,
            }
        }).collect()
    };

    // Soportes por volumen buy
    soportes.sort_by(|a, b| b.1.vol_buy.partial_cmp(&a.1.vol_buy)
        .unwrap_or(std::cmp::Ordering::Equal));
    let sv3 = top3(&soportes, "soporte", true);

    // Soportes por trades
    soportes.sort_by(|a, b| b.1.trades.cmp(&a.1.trades));
    let st3 = top3(&soportes, "soporte", true);

    // Resistencias por volumen sell
    resistencias.sort_by(|a, b| b.1.vol_sell.partial_cmp(&a.1.vol_sell)
        .unwrap_or(std::cmp::Ordering::Equal));
    let rv3 = top3(&resistencias, "resistencia", false);

    // Resistencias por trades
    resistencias.sort_by(|a, b| b.1.trades.cmp(&a.1.trades));
    let rt3 = top3(&resistencias, "resistencia", false);

    SRVentana {
        horas,
        precio_ref,
        bucket_size,
        soportes_vol: sv3,
        soportes_trades: st3,
        resistencias_vol: rv3,
        resistencias_trades: rt3,
    }
}

// ───────────────────────────────────────────────────────
// CVD por ventana
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Default)]
pub struct CVDVentana {
    pub horas: u64,
    pub cvd: f64,
    pub vol_buy: f64,
    pub vol_sell: f64,
    pub total: f64,
    pub trades: u64,
    pub dominancia: String,
}

pub fn analizar_cvd_ventanas<'a>(
    trades: impl Iterator<Item = &'a TradeConSeg> + Clone,
) -> Vec<CVDVentana> {
    VENTANAS_HORAS.iter().map(|&h| {
        let corte = corte_ms(h);
        let mut vol_buy = 0.0f64;
        let mut vol_sell = 0.0f64;
        let mut count = 0u64;

        for t in trades.clone() {
            if t.trade.timestamp < corte { continue; }
            count += 1;
            if t.trade.side == SIDE_BUY { vol_buy  += t.trade.volume_usdt; }
            else                  { vol_sell += t.trade.volume_usdt; }
        }

        let cvd   = vol_buy - vol_sell;
        let total = vol_buy + vol_sell;
        let dom   = if total == 0.0 { "EQ".into() }
                    else if vol_buy  > vol_sell * 1.05 { "BUY".into() }
                    else if vol_sell > vol_buy  * 1.05 { "SELL".into() }
                    else { "EQ".into() };

        CVDVentana { horas: h, cvd, vol_buy, vol_sell, total, trades: count, dominancia: dom }
    }).collect()
}

// ───────────────────────────────────────────────────────
// Divergencia P1-P3 vs P7-P9
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Default)]
pub struct DivergenciaRam {
    pub horas: u64,
    // Retail P1-P3
    pub retail_buy_pct: f64,
    pub retail_sell_pct: f64,
    pub retail_vol: f64,
    // Mid P4-P6
    pub mid_buy_pct: f64,
    pub mid_sell_pct: f64,
    pub mid_vol: f64,
    // Smart P7-P9
    pub smart_buy_pct: f64,
    pub smart_sell_pct: f64,
    pub smart_vol: f64,
    // Institucional P10-P12
    pub inst_buy_pct: f64,
    pub inst_sell_pct: f64,
    pub inst_vol: f64,
    // Divergencia retail vs smart
    pub divergencia: String,
    pub intensidad: f64,
}

pub fn analizar_divergencia<'a>(
    trades: impl Iterator<Item = &'a TradeConSeg> + Clone,
) -> Vec<DivergenciaRam> {
    VENTANAS_HORAS.iter().map(|&h| {
        let corte = corte_ms(h);
        let mut grupos = [[0.0f64; 2]; 4]; // [buy, sell] x [retail, mid, smart, inst]
        let mut vols   = [0.0f64; 4];

        for t in trades.clone() {
            if t.trade.timestamp < corte { continue; }
            let seg = t.seg as usize;
            let grupo = if seg <= 3 { 0 }       // P1-P3 retail
                        else if seg <= 6 { 1 }  // P4-P6 mid
                        else if seg <= 9 { 2 }  // P7-P9 smart
                        else if seg <= 12 { 3 } // P10-P12 inst
                        else { continue };

            vols[grupo] += t.trade.volume_usdt;
            if t.trade.side == SIDE_BUY { grupos[grupo][0] += t.trade.volume_usdt; }
            else                  { grupos[grupo][1] += t.trade.volume_usdt; }
        }

        let pct = |g: usize, i: usize| -> f64 {
            if vols[g] > 0.0 { grupos[g][i] / vols[g] * 100.0 } else { 50.0 }
        };

        let retail_buy = pct(0, 0);
        let smart_buy  = pct(2, 0);
        let delta = smart_buy - retail_buy;
        let div = if delta.abs() < 5.0 { "NEUTRAL".into() }
                  else if delta > 0.0  { "SMART_COMPRA".into() }
                  else                 { "SMART_VENDE".into() };

        DivergenciaRam {
            horas: h,
            retail_buy_pct: retail_buy,
            retail_sell_pct: pct(0, 1),
            retail_vol: vols[0],
            mid_buy_pct: pct(1, 0),
            mid_sell_pct: pct(1, 1),
            mid_vol: vols[1],
            smart_buy_pct: smart_buy,
            smart_sell_pct: pct(2, 1),
            smart_vol: vols[2],
            inst_buy_pct: pct(3, 0),
            inst_sell_pct: pct(3, 1),
            inst_vol: vols[3],
            divergencia: div,
            intensidad: delta.abs(),
        }
    }).collect()
}

// ───────────────────────────────────────────────────────
// Candles de 1 minuto para indicadores técnicos
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
struct Candle1m {
    ts_min: u64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    vol_usdt: f64,
    vol_buy: f64,
    trades: u64,
}

fn construir_candles<'a>(
    trades: impl Iterator<Item = &'a TradeConSeg>,
    seg_min: u8,
    seg_max: u8,
    horas: u64,
) -> Vec<Candle1m> {
    let corte = corte_ms(horas);
    let mut mapa: BTreeMap<u64, Candle1m> = BTreeMap::new();

    for t in trades {
        if t.trade.timestamp < corte { continue; }
        let seg = t.seg;
        if seg < seg_min || seg > seg_max { continue; }

        let min_key = t.trade.timestamp / 60_000;
        let e = mapa.entry(min_key).or_insert_with(|| Candle1m {
            ts_min: min_key * 60_000,
            open: t.trade.price,
            high: t.trade.price,
            low: t.trade.price,
            ..Default::default()
        });

        if t.trade.price > e.high { e.high = t.trade.price; }
        if t.trade.price < e.low  { e.low  = t.trade.price; }
        e.close     = t.trade.price;
        e.vol_usdt += t.trade.volume_usdt;
        e.trades   += 1;
        if t.trade.side == SIDE_BUY { e.vol_buy += t.trade.volume_usdt; }
    }

    mapa.into_values().collect()
}

// ───────────────────────────────────────────────────────
// RSI (14 períodos)
// ───────────────────────────────────────────────────────

fn calcular_rsi(candles: &[Candle1m], periodo: usize) -> f64 {
    if candles.len() < periodo + 1 { return 50.0; }
    let n = candles.len();
    let start = n.saturating_sub(periodo * 3).max(1);

    let mut gains = 0.0f64;
    let mut losses = 0.0f64;
    let mut count = 0usize;

    for i in start..n {
        let delta = candles[i].close - candles[i - 1].close;
        if delta > 0.0 { gains  += delta; }
        else           { losses -= delta; }
        count += 1;
    }

    if count == 0 || losses == 0.0 { return if gains > 0.0 { 100.0 } else { 50.0 }; }

    let avg_gain = gains  / count as f64;
    let avg_loss = losses / count as f64;
    let rs = avg_gain / avg_loss;
    100.0 - (100.0 / (1.0 + rs))
}

// ───────────────────────────────────────────────────────
// EMA auxiliar
// ───────────────────────────────────────────────────────

fn ema_serie(precios: &[f64], periodo: usize) -> Vec<f64> {
    if precios.is_empty() || periodo == 0 { return vec![]; }
    let k = 2.0 / (periodo as f64 + 1.0);
    let mut result = vec![0.0f64; precios.len()];
    result[0] = precios[0];
    for i in 1..precios.len() {
        result[i] = precios[i] * k + result[i - 1] * (1.0 - k);
    }
    result
}

// ───────────────────────────────────────────────────────
// MACD (12/26/9)
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Default)]
pub struct MacdResult {
    pub macd: f64,
    pub signal: f64,
    pub histogram: f64,
}

fn calcular_macd(candles: &[Candle1m]) -> MacdResult {
    if candles.len() < 26 { return MacdResult::default(); }
    let precios: Vec<f64> = candles.iter().map(|c| c.close).collect();
    let ema12 = ema_serie(&precios, 12);
    let ema26 = ema_serie(&precios, 26);

    let macd_serie: Vec<f64> = ema12.iter().zip(ema26.iter())
        .map(|(a, b)| a - b)
        .collect();

    let signal_serie = ema_serie(&macd_serie, 9);

    let macd   = *macd_serie.last().unwrap_or(&0.0);
    let signal = *signal_serie.last().unwrap_or(&0.0);

    MacdResult { macd, signal, histogram: macd - signal }
}

// ───────────────────────────────────────────────────────
// VWAP
// ───────────────────────────────────────────────────────

fn calcular_vwap(candles: &[Candle1m]) -> f64 {
    let (pv, vol) = candles.iter().fold((0.0f64, 0.0f64), |(pv, v), c| {
        let typical = (c.high + c.low + c.close) / 3.0;
        (pv + typical * c.vol_usdt, v + c.vol_usdt)
    });
    if vol > 0.0 { pv / vol } else { 0.0 }
}

// ───────────────────────────────────────────────────────
// Precio promedio bid/sell por grupo
// ───────────────────────────────────────────────────────

fn precio_prom_bid_sell<'a>(
    trades: impl Iterator<Item = &'a TradeConSeg>,
    seg_min: u8,
    seg_max: u8,
    horas: u64,
) -> (f64, f64) {
    let corte = corte_ms(horas);
    let mut sum_buy = 0.0f64; let mut cnt_buy = 0u64;
    let mut sum_sell= 0.0f64; let mut cnt_sell= 0u64;

    for t in trades {
        if t.trade.timestamp < corte { continue; }
        let seg = t.seg;
        if seg < seg_min || seg > seg_max { continue; }
        if t.trade.side == SIDE_BUY { sum_buy  += t.trade.price; cnt_buy  += 1; }
        else                  { sum_sell += t.trade.price; cnt_sell += 1; }
    }

    let avg_buy  = if cnt_buy  > 0 { sum_buy  / cnt_buy  as f64 } else { 0.0 };
    let avg_sell = if cnt_sell > 0 { sum_sell / cnt_sell as f64 } else { 0.0 };
    (avg_buy, avg_sell)
}

// ───────────────────────────────────────────────────────
// Indicadores por grupo de segmentos
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Default)]
pub struct IndicadoresGrupo {
    pub grupo: String,       // "P1-P3", "P4-P6", etc.
    pub seg_min: u8,
    pub seg_max: u8,
    pub rsi: f64,
    pub macd: MacdResult,
    pub vwap: f64,
    pub precio_prom_buy: f64,
    pub precio_prom_sell: f64,
    pub vol_total: f64,
    pub buy_pct: f64,
    pub trades: u64,
}

pub fn analizar_indicadores_grupos<'a>(
    trades_fn: impl Fn() -> Vec<&'a TradeConSeg>,
    horas: u64,
) -> Vec<IndicadoresGrupo> {
    let grupos = [
        ("P1-P3",  1u8,  3u8),
        ("P4-P6",  4u8,  6u8),
        ("P7-P9",  7u8,  9u8),
        ("P10-P12",10u8, 12u8),
    ];

    grupos.iter().map(|(nombre, seg_min, seg_max)| {
        let trades = trades_fn();
        let candles = construir_candles(trades.iter().copied(), *seg_min, *seg_max, horas);

        let rsi  = calcular_rsi(&candles, 14);
        let macd = calcular_macd(&candles);
        let vwap = calcular_vwap(&candles);

        let trades2 = trades_fn();
        let (pb, ps) = precio_prom_bid_sell(
            trades2.iter().copied(), *seg_min, *seg_max, horas
        );

        // Stats básicas del grupo
        let corte = corte_ms(horas);
        let mut vol_total = 0.0f64;
        let mut vol_buy   = 0.0f64;
        let mut trade_cnt = 0u64;

        for t in trades_fn() {
            if t.trade.timestamp < corte { continue; }
            let seg = t.seg;
            if seg < *seg_min || seg > *seg_max { continue; }
            vol_total += t.trade.volume_usdt;
            trade_cnt += 1;
            if t.trade.side == SIDE_BUY { vol_buy += t.trade.volume_usdt; }
        }

        let buy_pct = if vol_total > 0.0 { vol_buy / vol_total * 100.0 } else { 50.0 };

        IndicadoresGrupo {
            grupo: nombre.to_string(),
            seg_min: *seg_min,
            seg_max: *seg_max,
            rsi,
            macd,
            vwap,
            precio_prom_buy: pb,
            precio_prom_sell: ps,
            vol_total,
            buy_pct,
            trades: trade_cnt,
        }
    }).collect()
}

// ───────────────────────────────────────────────────────
// VWAP por sesión
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Default)]
pub struct VWAPSesion {
    pub nombre: String,
    pub vwap: f64,
    pub vol_total: f64,
    pub buy_pct: f64,
    pub trades: u64,
    pub precio_actual_vs_vwap: f64, // precio_ref - vwap
}

pub fn analizar_vwap_sesiones<'a>(
    trades: impl Iterator<Item = &'a TradeConSeg> + Clone,
    precio_actual: f64,
) -> Vec<VWAPSesion> {
    // Sesiones UTC: Asia 20-04, Londres 04-12, NY 12-20
    let sesiones = [
        ("Asia",     20u32, 4u32),
        ("Londres",  4u32,  12u32),
        ("NY",       12u32, 20u32),
    ];

    let ahora = chrono::Utc::now();
    let hora_utc = ahora.hour();

    sesiones.iter().map(|(nombre, h_ini, h_fin)| {
        // Calcular timestamp de inicio de esta sesión hoy
        let sesion_inicio_ms = {
            let hoy = ahora.date_naive();
            // Si la sesión cruza medianoche (Asia 20-04)
            let ini_dt = if *h_ini > *h_fin {
                // La sesión comenzó ayer a h_ini
                let ayer = hoy.pred_opt().unwrap_or(hoy);
                chrono::NaiveDateTime::new(
                    ayer,
                    chrono::NaiveTime::from_hms_opt(*h_ini, 0, 0).unwrap(),
                )
            } else {
                chrono::NaiveDateTime::new(
                    hoy,
                    chrono::NaiveTime::from_hms_opt(*h_ini, 0, 0).unwrap(),
                )
            };
            ini_dt.and_utc().timestamp_millis() as u64
        };

        let mut pv   = 0.0f64;
        let mut vol  = 0.0f64;
        let mut vbuy = 0.0f64;
        let mut cnt  = 0u64;

        for t in trades.clone() {
            if t.trade.timestamp < sesion_inicio_ms { continue; }
            let typical = t.trade.price; // aproximación sin OHLC
            pv  += typical * t.trade.volume_usdt;
            vol += t.trade.volume_usdt;
            cnt += 1;
            if t.trade.side == SIDE_BUY { vbuy += t.trade.volume_usdt; }
        }

        let vwap    = if vol > 0.0 { pv / vol } else { 0.0 };
        let buy_pct = if vol > 0.0 { vbuy / vol * 100.0 } else { 50.0 };

        VWAPSesion {
            nombre: nombre.to_string(),
            vwap,
            vol_total: vol,
            buy_pct,
            trades: cnt,
            precio_actual_vs_vwap: if vwap > 0.0 { precio_actual - vwap } else { 0.0 },
        }
    }).collect()
}

// ───────────────────────────────────────────────────────
// Resultado global del análisis RAM
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Default)]
pub struct AnalisisRAM {
    pub mercado: String,
    pub total_trades_ram: usize,
    pub precio_ref: f64,
    // S/R por ventanas
    pub sr_ventanas: Vec<SRVentana>,
    // CVD por ventanas
    pub cvd_ventanas: Vec<CVDVentana>,
    // Divergencia por ventanas
    pub divergencia: Vec<DivergenciaRam>,
    // Indicadores por grupo (ventana 4h por defecto)
    pub indicadores_grupos: Vec<IndicadoresGrupo>,
    // VWAP por sesión
    pub vwap_sesiones: Vec<VWAPSesion>,
}

use chrono::Timelike;
