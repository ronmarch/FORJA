// ═══════════════════════════════════════════════════════
// FORJA v1.0 - Order Book Futures + Liquidaciones
// ═══════════════════════════════════════════════════════
// Streams:
//   depth@100ms  → libro local profundo sincronizado con snapshot REST
//   forceOrder   → liquidaciones segmentadas P1-P14
// ═══════════════════════════════════════════════════════

use crate::segmentacion::segmentacion::{NOMBRES_CORTOS, TOTAL_SEGMENTOS};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::sync::RwLock;
use tokio_tungstenite::connect_async;
use tracing::{error, info, warn};

const BINANCE_FUTURES_WS: &str = "wss://fstream.binance.com/ws";
const BINANCE_FUTURES_REST: &str = "https://fapi.binance.com";
const RECONEXION_SEG: u64 = 5;
const SNAPSHOT_LIMIT: u16 = 1000;

// ───────────────────────────────────────────────────────
// Order Book local profundo
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Default)]
pub struct NivelPrecio {
    pub precio: f64,
    pub cantidad: f64,
    pub usdt: f64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct OrderBookState {
    pub bids: Vec<NivelPrecio>,
    pub asks: Vec<NivelPrecio>,
    pub bid_total_usdt: f64,
    pub ask_total_usdt: f64,
    pub bid_pct: f64,
    pub ask_pct: f64,
    pub spread: f64,
    pub spread_pct: f64,
    pub mid_price: f64,
    pub actualizaciones: u64,
    #[serde(skip)]
    bids_map: HashMap<String, f64>,
    #[serde(skip)]
    asks_map: HashMap<String, f64>,
    #[serde(skip)]
    last_update_id: u64,
    #[serde(skip)]
    last_stream_u: u64,
    #[serde(skip)]
    sincronizado: bool,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ParCVD {
    pub paso: usize,
    pub ask_precio: f64,
    pub ask_usdt: f64,
    pub bid_precio: f64,
    pub bid_usdt: f64,
    pub cvd: f64,
    pub cvd_pct: f64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct VentanaLiq {
    pub label: String,
    pub minutos: u64,
    pub long_usdt: f64,
    pub short_usdt: f64,
    pub long_count: u64,
    pub short_count: u64,
    pub total_usdt: f64,
    pub dominancia: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct CvdEscalonLiq {
    pub precio: f64,           // escalón entero (floor)
    pub long_usdt: f64,        // liquidaciones long en ese nivel
    pub short_usdt: f64,       // liquidaciones short en ese nivel
    pub cvd: f64,              // long - short (pos=más longs destruidos, neg=más shorts)
    pub total_usdt: f64,
    pub count: u64,
}

#[derive(Debug, Clone, Default)]
struct BucketNivel {
    cantidad: f64,
    usdt: f64,
}

#[derive(Debug, Deserialize)]
struct DepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize, Clone)]
struct DiffDepthMsg {
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    final_update_id: u64,
    #[serde(rename = "pu")]
    prev_final_update_id: u64,
    #[serde(default, rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(default, rename = "a")]
    asks: Vec<[String; 2]>,
}

impl OrderBookState {
    pub fn reset_local(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.bid_total_usdt = 0.0;
        self.ask_total_usdt = 0.0;
        self.bid_pct = 0.0;
        self.ask_pct = 0.0;
        self.spread = 0.0;
        self.spread_pct = 0.0;
        self.mid_price = 0.0;
        self.actualizaciones = 0;
        self.bids_map.clear();
        self.asks_map.clear();
        self.last_update_id = 0;
        self.last_stream_u = 0;
        self.sincronizado = false;
    }

    pub fn cargar_snapshot(&mut self, snapshot: DepthSnapshot) {
        self.reset_local();
        self.last_update_id = snapshot.last_update_id;

        for nivel in snapshot.bids {
            self.actualizar_lado(true, &nivel[0], &nivel[1]);
        }
        for nivel in snapshot.asks {
            self.actualizar_lado(false, &nivel[0], &nivel[1]);
        }

        self.reconstruir_vistas();
    }

    pub fn aplicar_diff(&mut self, diff: &DiffDepthMsg) -> Result<(), &'static str> {
        if diff.final_update_id < self.last_update_id {
            return Ok(());
        }

        if self.last_stream_u == 0 {
            if !(diff.first_update_id <= self.last_update_id
                && diff.final_update_id >= self.last_update_id)
            {
                return Err("primer_evento_desalineado");
            }
        } else if diff.prev_final_update_id != self.last_stream_u {
            return Err("secuencia_pu_invalida");
        }

        for nivel in &diff.bids {
            self.actualizar_lado(true, &nivel[0], &nivel[1]);
        }
        for nivel in &diff.asks {
            self.actualizar_lado(false, &nivel[0], &nivel[1]);
        }

        self.last_update_id = diff.final_update_id;
        self.last_stream_u = diff.final_update_id;
        self.sincronizado = true;
        self.actualizaciones += 1;
        self.reconstruir_vistas();

        Ok(())
    }

    fn precio_ancla(&self, precio_ref: f64) -> f64 {
        if precio_ref > 0.0 {
            precio_ref
        } else if self.mid_price > 0.0 {
            self.mid_price
        } else if !self.bids.is_empty() && !self.asks.is_empty() {
            (self.bids[0].precio + self.asks[0].precio) / 2.0
        } else {
            0.0
        }
    }

    fn agrupar_lado_entero(
        &self,
        niveles: &[NivelPrecio],
        precio_ref: f64,
        es_bid: bool,
        profundidad: usize,
    ) -> Vec<NivelPrecio> {
        let ancla = self.precio_ancla(precio_ref).floor() as i64;
        if ancla <= 0 {
            return Vec::new();
        }

        let mut buckets: BTreeMap<i64, BucketNivel> = BTreeMap::new();

        for n in niveles {
            let bucket = if es_bid {
                n.precio.floor() as i64
            } else {
                n.precio.ceil() as i64
            };

            if es_bid {
                if bucket >= ancla || bucket < ancla - profundidad as i64 {
                    continue;
                }
            } else if bucket <= ancla || bucket > ancla + profundidad as i64 {
                continue;
            }

            let acc = buckets.entry(bucket).or_default();
            acc.cantidad += n.cantidad;
            acc.usdt += n.usdt;
        }

        (1..=profundidad)
            .map(|paso| {
                let precio_bucket = if es_bid {
                    ancla - paso as i64
                } else {
                    ancla + paso as i64
                };

                if let Some(v) = buckets.get(&precio_bucket) {
                    NivelPrecio {
                        precio: precio_bucket as f64,
                        cantidad: v.cantidad.round(),
                        usdt: v.usdt.round(),
                    }
                } else {
                    NivelPrecio {
                        precio: precio_bucket as f64,
                        cantidad: 0.0,
                        usdt: 0.0,
                    }
                }
            })
            .collect()
    }

    pub fn niveles_enteros(
        &self,
        precio_ref: f64,
        profundidad: usize,
    ) -> (Vec<NivelPrecio>, Vec<NivelPrecio>) {
        (
            self.agrupar_lado_entero(&self.bids, precio_ref, true, profundidad),
            self.agrupar_lado_entero(&self.asks, precio_ref, false, profundidad),
        )
    }

    pub fn ratio_enteros(&self, precio_ref: f64, profundidad: usize) -> (f64, f64, f64, f64) {
        let (bids, asks) = self.niveles_enteros(precio_ref, profundidad);

        let bid_total = bids.iter().map(|n| n.usdt).sum::<f64>();
        let ask_total = asks.iter().map(|n| n.usdt).sum::<f64>();
        let total = bid_total + ask_total;

        if total > 0.0 {
            (
                bid_total,
                ask_total,
                (bid_total / total) * 100.0,
                (ask_total / total) * 100.0,
            )
        } else {
            (0.0, 0.0, 0.0, 0.0)
        }
    }

    pub fn cvd_global(&self) -> f64 {
        self.bid_total_usdt - self.ask_total_usdt
    }

    pub fn pares_cvd(&self, precio_ref: f64, profundidad: usize) -> Vec<ParCVD> {
        let (bids, asks) = self.niveles_enteros(precio_ref, profundidad);
        (0..profundidad).map(|i| {
            let ask = asks.get(i).cloned().unwrap_or_default();
            let bid = bids.get(i).cloned().unwrap_or_default();
            let cvd = ask.usdt - bid.usdt;
            let total = ask.usdt + bid.usdt;
            let cvd_pct = if total > 0.0 { (cvd / total) * 100.0 } else { 0.0 };
            ParCVD { paso: i+1, ask_precio: ask.precio, ask_usdt: ask.usdt,
                bid_precio: bid.precio, bid_usdt: bid.usdt, cvd, cvd_pct }
        }).collect()
    }

    fn actualizar_lado(&mut self, es_bid: bool, precio_raw: &str, qty_raw: &str) {
        let Some(precio_key) = normalizar_precio(precio_raw) else {
            return;
        };
        let qty = qty_raw.parse::<f64>().unwrap_or(0.0);

        let lado = if es_bid {
            &mut self.bids_map
        } else {
            &mut self.asks_map
        };

        if qty <= 0.0 {
            lado.remove(&precio_key);
        } else {
            lado.insert(precio_key, qty);
        }
    }

    fn reconstruir_vistas(&mut self) {
        self.bids = materializar_lado(&self.bids_map, true);
        self.asks = materializar_lado(&self.asks_map, false);

        self.bid_total_usdt = self.bids.iter().map(|n| n.usdt).sum();
        self.ask_total_usdt = self.asks.iter().map(|n| n.usdt).sum();

        let total = self.bid_total_usdt + self.ask_total_usdt;
        if total > 0.0 {
            self.bid_pct = (self.bid_total_usdt / total) * 100.0;
            self.ask_pct = (self.ask_total_usdt / total) * 100.0;
        } else {
            self.bid_pct = 0.0;
            self.ask_pct = 0.0;
        }

        if !self.bids.is_empty() && !self.asks.is_empty() {
            let bb = self.bids[0].precio;
            let ba = self.asks[0].precio;
            self.spread = ba - bb;
            self.mid_price = (bb + ba) / 2.0;
            if self.mid_price > 0.0 {
                self.spread_pct = (self.spread / self.mid_price) * 100.0;
            } else {
                self.spread_pct = 0.0;
            }
        } else {
            self.spread = 0.0;
            self.spread_pct = 0.0;
            self.mid_price = 0.0;
        }
    }
}

fn normalizar_precio(precio_raw: &str) -> Option<String> {
    let precio = precio_raw.parse::<f64>().ok()?;
    Some(format!("{:.8}", precio))
}

fn materializar_lado(mapa: &HashMap<String, f64>, es_bid: bool) -> Vec<NivelPrecio> {
    let mut niveles: Vec<NivelPrecio> = mapa
        .iter()
        .filter_map(|(precio_key, qty)| {
            if *qty <= 0.0 {
                return None;
            }
            let precio = precio_key.parse::<f64>().ok()?;
            Some(NivelPrecio {
                precio,
                cantidad: *qty,
                usdt: precio * *qty,
            })
        })
        .collect();

    niveles.sort_by(|a, b| {
        let ord = a.precio.partial_cmp(&b.precio).unwrap_or(Ordering::Equal);
        if es_bid { ord.reverse() } else { ord }
    });

    niveles
}

async fn descargar_snapshot(simbolo: &str) -> Result<DepthSnapshot, String> {
    let url = format!(
        "{}/fapi/v1/depth?symbol={}&limit={}",
        BINANCE_FUTURES_REST, simbolo, SNAPSHOT_LIMIT
    );

    let respuesta = reqwest::get(&url)
        .await
        .map_err(|e| format!("snapshot request: {}", e))?;

    if !respuesta.status().is_success() {
        return Err(format!("snapshot HTTP {}", respuesta.status()));
    }

    respuesta
        .json::<DepthSnapshot>()
        .await
        .map_err(|e| format!("snapshot parse: {}", e))
}

async fn sincronizar_orderbook_local(
    simbolo: &str,
    state: &Arc<RwLock<OrderBookState>>,
) -> Result<(), String> {
    let snapshot = descargar_snapshot(simbolo).await?;
    let mut ob = state.write().await;
    ob.cargar_snapshot(snapshot);
    Ok(())
}

// ───────────────────────────────────────────────────────
// Liquidaciones
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct Liquidacion {
    pub timestamp: u64,
    pub precio: f64,
    pub cantidad_usdt: f64,
    pub side: String,
    pub segmento: String,
    pub segment_id: u8,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct LiquidacionesState {
    pub recientes: Vec<Liquidacion>,
    pub por_segmento: Vec<SegmentoLiq>,
    pub total_long_liq: f64,
    pub total_short_liq: f64,
    pub total_long_count: u64,
    pub total_short_count: u64,
    pub total_count: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct SegmentoLiq {
    pub nombre: String,
    pub long_count: u64,
    pub short_count: u64,
    pub long_usdt: f64,
    pub short_usdt: f64,
}

impl LiquidacionesState {
    pub fn cvd_escalones(&self, ventana_minutos: u64) -> Vec<CvdEscalonLiq> {
        let ahora_ms = chrono::Utc::now().timestamp_millis() as u64;
        let corte = ahora_ms.saturating_sub(ventana_minutos * 60 * 1000);
        let mut mapa: std::collections::BTreeMap<i64, CvdEscalonLiq> = std::collections::BTreeMap::new();

        for liq in &self.recientes {
            if liq.timestamp < corte { continue; }
            let nivel = liq.precio.floor() as i64;
            let entry = mapa.entry(nivel).or_insert_with(|| CvdEscalonLiq {
                precio: nivel as f64,
                ..Default::default()
            });
            entry.total_usdt += liq.cantidad_usdt;
            entry.count      += 1;
            if liq.side.contains("LONG") { entry.long_usdt  += liq.cantidad_usdt; }
            else                         { entry.short_usdt += liq.cantidad_usdt; }
        }

        let mut resultado: Vec<CvdEscalonLiq> = mapa.into_values().map(|mut e| {
            e.cvd = e.long_usdt - e.short_usdt;
            e
        }).collect();
        // Ordenar de mayor precio a menor (como orderbook)
        resultado.sort_by(|a, b| b.precio.partial_cmp(&a.precio).unwrap_or(std::cmp::Ordering::Equal));
        resultado
    }

    pub fn calcular_ventanas(&self) -> Vec<VentanaLiq> {
        let ahora_ms = chrono::Utc::now().timestamp_millis() as u64;
        let defs: &[(&str, u64)] = &[("15m",15),("1h",60),("2h",120),("4h",240),("24h",1440)];
        defs.iter().map(|(label, minutos)| {
            let corte = ahora_ms.saturating_sub(minutos * 60 * 1000);
            let mut long_usdt = 0.0f64; let mut short_usdt = 0.0f64;
            let mut long_count = 0u64; let mut short_count = 0u64;
            for liq in &self.recientes {
                if liq.timestamp < corte { continue; }
                if liq.side.contains("LONG") { long_usdt += liq.cantidad_usdt; long_count += 1; }
                else { short_usdt += liq.cantidad_usdt; short_count += 1; }
            }
            let total_usdt = long_usdt + short_usdt;
            let dominancia = if total_usdt == 0.0 { "EQ".into() }
                else if long_usdt > short_usdt * 1.1 { "LONG".into() }
                else if short_usdt > long_usdt * 1.1 { "SHORT".into() }
                else { "EQ".into() };
            VentanaLiq { label: label.to_string(), minutos: *minutos,
                long_usdt, short_usdt, long_count, short_count, total_usdt, dominancia }
        }).collect()
    }
}

#[derive(Deserialize)]
struct ForceOrderMsg {
    o: ForceOrderData,
}

#[derive(Deserialize)]
struct ForceOrderData {
    #[serde(rename = "S")]
    side: String,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    qty: String,
    #[serde(rename = "T")]
    trade_time: u64,
}

// ───────────────────────────────────────────────────────
// Estado completo M7
// ───────────────────────────────────────────────────────

#[derive(Clone)]
pub struct M7State {
    pub orderbook: Arc<RwLock<OrderBookState>>,
    pub liquidaciones: Arc<RwLock<LiquidacionesState>>,
}

impl M7State {
    pub fn new() -> Self {
        let mut liq_state = LiquidacionesState::default();
        liq_state.por_segmento = (0..TOTAL_SEGMENTOS)
            .map(|i| SegmentoLiq {
                nombre: NOMBRES_CORTOS[i].to_string(),
                ..Default::default()
            })
            .collect();

        Self {
            orderbook: Arc::new(RwLock::new(OrderBookState::default())),
            liquidaciones: Arc::new(RwLock::new(liq_state)),
        }
    }
}

// ───────────────────────────────────────────────────────
// Clasificar liquidación por segmento
// ───────────────────────────────────────────────────────

fn clasificar_segmento(usdt: f64) -> u8 {
    if usdt < 40.0 {
        1
    } else if usdt < 100.0 {
        2
    } else if usdt < 500.0 {
        3
    } else if usdt < 1_000.0 {
        4
    } else if usdt < 2_500.0 {
        5
    } else if usdt < 5_000.0 {
        6
    } else if usdt < 10_000.0 {
        7
    } else if usdt < 50_000.0 {
        8
    } else if usdt < 100_000.0 {
        9
    } else if usdt < 500_000.0 {
        10
    } else if usdt < 1_000_000.0 {
        11
    } else if usdt < 5_000_000.0 {
        12
    } else if usdt < 10_000_000.0 {
        13
    } else {
        14
    }
}

// ───────────────────────────────────────────────────────
// Stream de Order Book local profundo
// ───────────────────────────────────────────────────────

pub async fn iniciar_depth_stream(
    simbolo: String,
    state: Arc<RwLock<OrderBookState>>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let url = format!("{}/{}@depth@100ms", BINANCE_FUTURES_WS, simbolo.to_lowercase());

    loop {
        if *stop.borrow() {
            return;
        }

        {
            let mut ob = state.write().await;
            ob.reset_local();
        }

        info!("ORDERBOOK | Conectando diff depth: {}", url);

        match connect_async(&url).await {
            Ok((ws, _)) => {
                info!("ORDERBOOK | Diff depth conectado ✓");

                if let Err(e) = sincronizar_orderbook_local(&simbolo, &state).await {
                    error!("ORDERBOOK | Snapshot inicial falló: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(RECONEXION_SEG)).await;
                    continue;
                }

                info!(
                    "ORDERBOOK | Snapshot sincronizado ✓ | {} | límite {}",
                    simbolo, SNAPSHOT_LIMIT
                );

                let (_, mut read) = ws.split();

                loop {
                    tokio::select! {
                        msg = read.next() => {
                            match msg {
                                Some(Ok(m)) if m.is_text() => {
                                    let txt = m.to_text().unwrap_or("");
                                    if let Ok(diff) = serde_json::from_str::<DiffDepthMsg>(txt) {
                                        let resultado = {
                                            let mut ob = state.write().await;
                                            ob.aplicar_diff(&diff)
                                        };

                                        if let Err(motivo) = resultado {
                                            warn!("ORDERBOOK | Resync requerido: {}", motivo);
                                            if let Err(e) = sincronizar_orderbook_local(&simbolo, &state).await {
                                                error!("ORDERBOOK | Resync falló: {}", e);
                                                break;
                                            }
                                        }
                                    }
                                }
                                Some(Err(e)) => {
                                    warn!("ORDERBOOK | Error: {}", e);
                                    break;
                                }
                                None => break,
                                _ => {}
                            }
                        }
                        _ = stop.changed() => {
                            if *stop.borrow() {
                                return;
                            }
                        }
                    }
                }
            }
            Err(e) => error!("ORDERBOOK | Error conectar: {}", e),
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(RECONEXION_SEG)).await;
    }
}

// ───────────────────────────────────────────────────────
// Stream de Liquidaciones
// ───────────────────────────────────────────────────────

pub async fn iniciar_liquidaciones_stream(
    simbolo: String,
    state: Arc<RwLock<LiquidacionesState>>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let url = format!("{}/{}@forceOrder", BINANCE_FUTURES_WS, simbolo.to_lowercase());

    loop {
        if *stop.borrow() {
            return;
        }

        info!("LIQUIDACIONES | Conectando: {}", url);

        match connect_async(&url).await {
            Ok((ws, _)) => {
                info!("LIQUIDACIONES | Conectado ✓");
                let (mut write, mut read) = ws.split();
                let mut ping_iv = tokio::time::interval(tokio::time::Duration::from_secs(20));
                ping_iv.tick().await;

                loop {
                    tokio::select! {
                        _ = ping_iv.tick() => {
                            use tokio_tungstenite::tungstenite::Message;
                            if let Err(e) = futures::SinkExt::send(&mut write, Message::Ping(vec![])).await {
                                warn!("LIQUIDACIONES | Ping error: {}", e);
                                break;
                            }
                        }
                        msg = read.next() => {
                            match msg {
                                Some(Ok(m)) if m.is_ping() => {
                                    use tokio_tungstenite::tungstenite::Message;
                                    let _ = futures::SinkExt::send(&mut write, Message::Pong(m.into_data())).await;
                                }
                                Some(Ok(m)) if m.is_text() => {
                                    let txt = m.to_text().unwrap_or("");
                                    tracing::debug!("LIQUIDACIONES | raw: {}", &txt[..txt.len().min(120)]);
                                    match serde_json::from_str::<ForceOrderMsg>(txt) { Ok(fo) => {
                                        let precio: f64 = fo.o.price.parse().unwrap_or(0.0);
                                        let qty: f64 = fo.o.qty.parse().unwrap_or(0.0);
                                        let usdt = precio * qty;
                                        let seg_id = clasificar_segmento(usdt);

                                        let es_long_liq = fo.o.side == "SELL";
                                        let side_str = if es_long_liq {
                                            "LONG liq".to_string()
                                        } else {
                                            "SHORT liq".to_string()
                                        };

                                        let liq = Liquidacion {
                                            timestamp: fo.o.trade_time,
                                            precio,
                                            cantidad_usdt: usdt,
                                            side: side_str,
                                            segmento: NOMBRES_CORTOS[(seg_id as usize)
                                                .saturating_sub(1)
                                                .min(TOTAL_SEGMENTOS - 1)]
                                                .to_string(),
                                            segment_id: seg_id,
                                        };

                                        let mut ls = state.write().await;

                                        ls.recientes.push(liq.clone());
                                        if ls.recientes.len() > 5000 {
                                            ls.recientes.remove(0);
                                        }

                                        let idx = (seg_id as usize).saturating_sub(1);
                                        if idx < ls.por_segmento.len() {
                                            if es_long_liq {
                                                ls.por_segmento[idx].long_count += 1;
                                                ls.por_segmento[idx].long_usdt += usdt;
                                                ls.total_long_liq += usdt;
                                                ls.total_long_count += 1;
                                            } else {
                                                ls.por_segmento[idx].short_count += 1;
                                                ls.por_segmento[idx].short_usdt += usdt;
                                                ls.total_short_liq += usdt;
                                                ls.total_short_count += 1;
                                            }
                                        }
                                        ls.total_count += 1;

                                        if usdt > 50_000.0 {
                                            tracing::warn!(
                                                "LIQUIDACIÓN | {} {} | ${:.2} | {}",
                                                liq.side, liq.segmento, usdt, simbolo
                                            );
                                        }
                                    }
                                    Err(e) => { tracing::debug!("LIQUIDACIONES | Parse fail: {} | raw: {}", e, &txt[..txt.len().min(120)]); }
                                    }
                                }
                                Some(Err(e)) => {
                                    warn!("LIQUIDACIONES | Error: {}", e);
                                    break;
                                }
                                None => break,
                                _ => {}
                            }
                        }
                        _ = stop.changed() => {
                            if *stop.borrow() {
                                return;
                            }
                        }
                    }
                }
            }
            Err(e) => error!("LIQUIDACIONES | Error conectar: {}", e),
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(RECONEXION_SEG)).await;
    }
}

/// Resetear liquidaciones (stop programado o cambio de par)
pub async fn resetear_liquidaciones(state: &Arc<RwLock<LiquidacionesState>>) {
    let mut ls = state.write().await;
    ls.recientes.clear();
    ls.total_long_liq = 0.0;
    ls.total_short_liq = 0.0;
    ls.total_long_count = 0;
    ls.total_short_count = 0;
    ls.total_count = 0;
    for seg in &mut ls.por_segmento {
        seg.long_count = 0;
        seg.short_count = 0;
        seg.long_usdt = 0.0;
        seg.short_usdt = 0.0;
    }
}
