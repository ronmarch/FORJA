// ═══════════════════════════════════════════════════════
// FORJA v1.0 - Soportes y Resistencias
// ═══════════════════════════════════════════════════════
// Análisis automático sobre Parquet de sesión.
// Buckets dinámicos = precio × 0.6%
// Top 3 S/R por volumen + Top 3 S/R por trades
// Guarda resultados en data/{PAR}/indicadores/sr.parquet
// ═══════════════════════════════════════════════════════

use crate::config::control::sesion_actual_utc;
use crate::segmentacion::segmentacion::seg_desde_vol;
use polars::prelude::*;
use arrow::array::{Float64Array, StringArray, UInt64Array, UInt8Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

const BUCKET_PCT: f64 = 0.006; // 0.6% del precio

// ───────────────────────────────────────────────────────
// Resultado de un nivel S/R
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NivelSR {
    pub precio_nivel: f64,
    pub volumen_usdt: f64,
    pub trade_count: u64,
    pub buy_pct: f64,
    pub sell_pct: f64,
    pub seg_dominante: u8,
    pub score: f64,
    pub criterio: String,   // "volumen" o "trades"
    pub tipo: String,       // "soporte" o "resistencia"
    pub rank: u8,           // 1, 2 o 3
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResultadoSR {
    pub timestamp: u64,
    pub sesion: String,
    pub par: String,
    pub precio_ref: f64,
    pub bucket_size: f64,
    pub soportes_vol: Vec<NivelSR>,
    pub soportes_trades: Vec<NivelSR>,
    pub resistencias_vol: Vec<NivelSR>,
    pub resistencias_trades: Vec<NivelSR>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArchivoParquet {
    pub nombre: String,
    pub ruta: String,
    pub mercado: String,
    pub tamano_kb: u64,
}

// ───────────────────────────────────────────────────────
// Bucket interno de cálculo
// ───────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
struct Bucket {
    precio_centro: f64,
    vol_total: f64,
    vol_buy: f64,
    vol_sell: f64,
    trades: u64,
    vol_por_seg: [f64; 14],
}

impl Bucket {
    fn seg_dominante(&self) -> u8 {
        let mut max = 0.0f64;
        let mut idx = 0usize;
        for (i, &v) in self.vol_por_seg.iter().enumerate() {
            if v > max { max = v; idx = i; }
        }
        (idx + 1) as u8
    }

    fn pct_buy(&self) -> f64 {
        if self.vol_total > 0.0 { (self.vol_buy / self.vol_total) * 100.0 } else { 0.0 }
    }

    fn pct_sell(&self) -> f64 {
        if self.vol_total > 0.0 { (self.vol_sell / self.vol_total) * 100.0 } else { 0.0 }
    }

    fn score_soporte(&self) -> f64 {
        self.vol_buy  // volumen comprador directo
    }

    fn score_resistencia(&self) -> f64 {
        self.vol_sell  // volumen vendedor directo
    }
}

// ───────────────────────────────────────────────────────
// Motor de análisis S/R
// ───────────────────────────────────────────────────────

pub struct MotorSR {
    data_dir: String,
}

impl MotorSR {
    pub fn new(data_dir: &str) -> Self {
        Self { data_dir: data_dir.to_string() }
    }

    /// Ejecuta el análisis S/R sobre el último Parquet de sesión
    pub fn analizar(&self, par: &str, mercado: &str) -> ResultadoSR {
        let dir = format!("{}/{}/{}", self.data_dir, par, mercado);
        let parquet_path = match self.ultimo_parquet(&dir) {
            Some(p) => p,
            None => {
                let ts = chrono::Utc::now().timestamp_millis() as u64;
                let (_, nombre_sesion) = sesion_actual_utc();
                return ResultadoSR {
                    timestamp: ts, sesion: nombre_sesion.to_string(),
                    par: par.to_string(),
                    error: Some(format!("No hay Parquet en {}", dir)),
                    ..Default::default()
                };
            }
        };
        self.analizar_ruta(&parquet_path, par)
    }

    /// Ejecuta el análisis S/R sobre un Parquet específico (análisis manual)
    pub fn analizar_ruta(&self, parquet_path: &str, par: &str) -> ResultadoSR {
        let ts = chrono::Utc::now().timestamp_millis() as u64;
        let (_, nombre_sesion) = sesion_actual_utc();

        tracing::info!("SR | Analizando: {}", parquet_path);

        // Leer con Polars
        let lf = match LazyFrame::scan_parquet(parquet_path, ScanArgsParquet::default()) {
            Ok(lf) => lf,
            Err(e) => return ResultadoSR {
                timestamp: ts, sesion: nombre_sesion.to_string(),
                par: par.to_string(),
                error: Some(format!("Error leyendo Parquet: {}", e)),
                ..Default::default()
            },
        };

        let df = match lf.collect() {
            Ok(df) => df,
            Err(e) => return ResultadoSR {
                timestamp: ts, sesion: nombre_sesion.to_string(),
                par: par.to_string(),
                error: Some(format!("Error collect: {}", e)),
                ..Default::default()
            },
        };

        if df.height() == 0 {
            return ResultadoSR {
                timestamp: ts, sesion: nombre_sesion.to_string(),
                par: par.to_string(),
                error: Some("Parquet vacío".to_string()),
                ..Default::default()
            };
        }

        // Extraer columnas
        let prices = match df.column("price").and_then(|c| c.f64()) {
            Ok(p) => p.clone(),
            Err(e) => return ResultadoSR {
                timestamp: ts, sesion: nombre_sesion.to_string(), par: par.to_string(),
                error: Some(format!("Error columna price: {}", e)),
                ..Default::default()
            },
        };
        let vols = match df.column("volume_usdt").and_then(|c| c.f64()) {
            Ok(v) => v.clone(),
            Err(e) => return ResultadoSR {
                timestamp: ts, sesion: nombre_sesion.to_string(), par: par.to_string(),
                error: Some(format!("Error columna volume_usdt: {}", e)),
                ..Default::default()
            },
        };
        let sides = match df.column("side").and_then(|c| c.u8()) {
            Ok(s) => s.clone(),
            Err(e) => return ResultadoSR {
                timestamp: ts, sesion: nombre_sesion.to_string(), par: par.to_string(),
                error: Some(format!("Error columna side: {}", e)),
                ..Default::default()
            },
        };

        // Columna seg (puede no existir en Parquet legacy)
        let segs = df.column("seg").and_then(|c| c.u8()).ok().cloned();

        // Precio de referencia = último precio del Parquet
        let precio_ref = prices.get(df.height() - 1).unwrap_or(0.0);
        if precio_ref <= 0.0 {
            return ResultadoSR {
                timestamp: ts, sesion: nombre_sesion.to_string(), par: par.to_string(),
                error: Some("Precio de referencia inválido".to_string()),
                ..Default::default()
            };
        }

        let bucket_size = precio_ref * BUCKET_PCT;

        tracing::info!("SR | Precio ref: {:.4} | Bucket: {:.4} | Trades: {}",
            precio_ref, bucket_size, df.height());

        // Construir buckets
        let mut buckets: BTreeMap<i64, Bucket> = BTreeMap::new();

        for i in 0..df.height() {
            let price = match prices.get(i) { Some(p) => p, None => continue };
            let vol = match vols.get(i) { Some(v) => v, None => continue };
            let side = match sides.get(i) { Some(s) => s, None => continue };
            let seg = segs.as_ref().and_then(|s| s.get(i)).unwrap_or_else(|| {
                // Calcular seg desde volume_usdt si no existe
                seg_desde_vol(vol)
            });

            // Índice del bucket
            let bucket_idx = (price / bucket_size).floor() as i64;
            let entry = buckets.entry(bucket_idx).or_insert_with(|| {
                Bucket {
                    precio_centro: (bucket_idx as f64 + 0.5) * bucket_size,
                    ..Default::default()
                }
            });

            entry.vol_total += vol;
            entry.trades += 1;
            if side == 0 { entry.vol_buy += vol; } else { entry.vol_sell += vol; }

            let seg_idx = (seg as usize).saturating_sub(1).min(13);
            entry.vol_por_seg[seg_idx] += vol;
        }

        // Separar soportes y resistencias
        let bucket_ref = (precio_ref / bucket_size).floor() as i64;

        let mut soportes: Vec<(i64, Bucket)> = buckets.iter()
            .filter(|(&k, _)| k < bucket_ref)
            .map(|(&k, b)| (k, b.clone()))
            .collect();
        let mut resistencias: Vec<(i64, Bucket)> = buckets.iter()
            .filter(|(&k, _)| k > bucket_ref)
            .map(|(&k, b)| (k, b.clone()))
            .collect();

        // Top 3 soportes por volumen (más cercanos al precio, con más volumen)
        soportes.sort_by(|a, b| b.1.vol_buy.partial_cmp(&a.1.vol_buy).unwrap_or(std::cmp::Ordering::Equal));
        let sv3 = top3_niveles(&soportes, "soporte", "volumen", true);

        // Top 3 soportes por trades
        soportes.sort_by(|a, b| b.1.trades.cmp(&a.1.trades));
        let st3 = top3_niveles(&soportes, "soporte", "trades", true);

        // Top 3 resistencias por volumen
        resistencias.sort_by(|a, b| b.1.vol_sell.partial_cmp(&a.1.vol_sell).unwrap_or(std::cmp::Ordering::Equal));
        let rv3 = top3_niveles(&resistencias, "resistencia", "volumen", false);

        // Top 3 resistencias por trades
        resistencias.sort_by(|a, b| b.1.trades.cmp(&a.1.trades));
        let rt3 = top3_niveles(&resistencias, "resistencia", "trades", false);

        let resultado = ResultadoSR {
            timestamp: ts,
            sesion: nombre_sesion.to_string(),
            par: par.to_string(),
            precio_ref,
            bucket_size,
            soportes_vol: sv3,
            soportes_trades: st3,
            resistencias_vol: rv3,
            resistencias_trades: rt3,
            error: None,
        };

        // Guardar en Parquet histórico
        if let Err(e) = self.guardar_resultado(&resultado) {
            tracing::error!("SR | Error guardando resultado: {}", e);
        } else {
            tracing::info!("SR | Resultado guardado para {}", par);
        }

        resultado
    }

    /// Guarda el resultado en el Parquet histórico de S/R
    pub fn guardar_resultado(&self, r: &ResultadoSR) -> Result<(), String> {
        let dir = format!("{}/{}/indicadores", self.data_dir, r.par);
        fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        let ruta = format!("{}/sr.parquet", dir);

        // Construir todos los niveles en vectores columna
        let todos: Vec<&NivelSR> = r.soportes_vol.iter()
            .chain(r.soportes_trades.iter())
            .chain(r.resistencias_vol.iter())
            .chain(r.resistencias_trades.iter())
            .collect();

        if todos.is_empty() { return Ok(()); }

        let n = todos.len();
        let timestamps: Vec<u64> = vec![r.timestamp; n];
        let sesiones: Vec<&str> = vec![r.sesion.as_str(); n];
        let pares: Vec<&str> = vec![r.par.as_str(); n];
        let precio_refs: Vec<f64> = vec![r.precio_ref; n];
        let bucket_sizes: Vec<f64> = vec![r.bucket_size; n];
        let tipos: Vec<&str> = todos.iter().map(|v| v.tipo.as_str()).collect();
        let criterios: Vec<&str> = todos.iter().map(|v| v.criterio.as_str()).collect();
        let ranks: Vec<u8> = todos.iter().map(|v| v.rank).collect();
        let precios: Vec<f64> = todos.iter().map(|v| v.precio_nivel).collect();
        let vols: Vec<f64> = todos.iter().map(|v| v.volumen_usdt).collect();
        let trades: Vec<u64> = todos.iter().map(|v| v.trade_count).collect();
        let buy_pcts: Vec<f64> = todos.iter().map(|v| v.buy_pct).collect();
        let sell_pcts: Vec<f64> = todos.iter().map(|v| v.sell_pct).collect();
        let segs: Vec<u8> = todos.iter().map(|v| v.seg_dominante).collect();
        let scores: Vec<f64> = todos.iter().map(|v| v.score).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::UInt64, false),
            Field::new("sesion", DataType::Utf8, false),
            Field::new("par", DataType::Utf8, false),
            Field::new("precio_ref", DataType::Float64, false),
            Field::new("bucket_size", DataType::Float64, false),
            Field::new("tipo", DataType::Utf8, false),
            Field::new("criterio", DataType::Utf8, false),
            Field::new("rank", DataType::UInt8, false),
            Field::new("precio_nivel", DataType::Float64, false),
            Field::new("volumen_usdt", DataType::Float64, false),
            Field::new("trade_count", DataType::UInt64, false),
            Field::new("buy_pct", DataType::Float64, false),
            Field::new("sell_pct", DataType::Float64, false),
            Field::new("seg_dominante", DataType::UInt8, false),
            Field::new("score", DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(UInt64Array::from(timestamps)),
            Arc::new(StringArray::from(sesiones)),
            Arc::new(StringArray::from(pares)),
            Arc::new(Float64Array::from(precio_refs)),
            Arc::new(Float64Array::from(bucket_sizes)),
            Arc::new(StringArray::from(tipos)),
            Arc::new(StringArray::from(criterios)),
            Arc::new(UInt8Array::from(ranks)),
            Arc::new(Float64Array::from(precios)),
            Arc::new(Float64Array::from(vols)),
            Arc::new(UInt64Array::from(trades)),
            Arc::new(Float64Array::from(buy_pcts)),
            Arc::new(Float64Array::from(sell_pcts)),
            Arc::new(UInt8Array::from(segs)),
            Arc::new(Float64Array::from(scores)),
        ]).map_err(|e| e.to_string())?;

        // Si ya existe el archivo, lo leemos y agregamos (append)
        if Path::new(&ruta).exists() {
            // Leer existente, combinar y reescribir
            let lf_existente = LazyFrame::scan_parquet(&ruta, ScanArgsParquet::default())
                .map_err(|e| e.to_string())?;
            let df_existente = lf_existente.collect().map_err(|e| e.to_string())?;

            // Escribir el batch nuevo en temporal
            let ruta_tmp = format!("{}/sr_tmp.parquet", dir);
            escribir_batch(&batch, &schema, &ruta_tmp)?;

            // Leer tmp y combinar
            let lf_nuevo = LazyFrame::scan_parquet(&ruta_tmp, ScanArgsParquet::default())
                .map_err(|e| e.to_string())?;
            let df_nuevo = lf_nuevo.collect().map_err(|e| e.to_string())?;

            let df_combinado = df_existente.vstack(&df_nuevo).map_err(|e| e.to_string())?;

            // Reescribir combinado
            // Reescribir combinado desde Polars (ArrowWriter no necesario aquí)
            fs::remove_file(&ruta_tmp).ok();

            // Reescribir todo desde Polars
            let ruta_final = ruta.clone();
            guardar_df_como_parquet(df_combinado, &ruta_final)?;

        } else {
            escribir_batch(&batch, &schema, &ruta)?;
        }

        tracing::info!("SR | Parquet histórico actualizado: {} ({} niveles)", ruta, n);
        Ok(())
    }

    /// Lista todos los Parquets disponibles para un par (spot + futures)
    pub fn listar_archivos(&self, par: &str) -> Vec<ArchivoParquet> {
        let mut archivos = Vec::new();
        for mercado in &["spot", "futures"] {
            let dir = format!("{}/{}/{}", self.data_dir, par, mercado);
            let path = Path::new(&dir);
            if !path.is_dir() { continue; }
            if let Ok(entries) = fs::read_dir(path) {
                let mut lista: Vec<ArchivoParquet> = entries
                    .flatten()
                    .filter(|e| e.path().extension().map(|x| x == "parquet").unwrap_or(false))
                    .map(|e| {
                        let ruta = e.path().to_string_lossy().to_string();
                        let nombre = e.file_name().to_string_lossy().to_string();
                        let tamano_kb = e.metadata().map(|m| m.len() / 1024).unwrap_or(0);
                        ArchivoParquet { nombre, ruta, mercado: mercado.to_string(), tamano_kb }
                    })
                    .collect();
                lista.sort_by(|a, b| b.nombre.cmp(&a.nombre));
                archivos.extend(lista);
            }
        }
        archivos
    }

    /// Último archivo Parquet en un directorio
    fn ultimo_parquet(&self, dir: &str) -> Option<String> {
        let path = Path::new(dir);
        if !path.is_dir() { return None; }
        let mut archivos: Vec<String> = fs::read_dir(path).ok()?
            .flatten()
            .filter(|e| e.path().extension().map(|x| x == "parquet").unwrap_or(false))
            .map(|e| e.path().to_string_lossy().to_string())
            .collect();
        archivos.sort();
        archivos.last().cloned()
    }

    /// Carga el último resultado desde el Parquet histórico
    pub fn ultimo_resultado(&self, par: &str) -> Option<ResultadoSR> {
        let ruta = format!("{}/{}/indicadores/sr.parquet", self.data_dir, par);
        if !Path::new(&ruta).exists() { return None; }

        let lf = LazyFrame::scan_parquet(&ruta, ScanArgsParquet::default()).ok()?;
        let df = lf.collect().ok()?;
        if df.height() == 0 { return None; }

        // Obtener el timestamp más reciente
        let ts_col = df.column("timestamp").ok()?.u64().ok()?;
        let max_ts = ts_col.max()?;

        // Filtrar solo las filas del último análisis
        let mask = ts_col.equal(max_ts);
        let df_ultimo = df.filter(&mask).ok()?;

        Some(df_a_resultado(&df_ultimo, max_ts))
    }
}

// ───────────────────────────────────────────────────────
// Helpers
// ───────────────────────────────────────────────────────

fn top3_niveles(
    buckets: &[(i64, Bucket)],
    tipo: &str,
    criterio: &str,
    es_soporte: bool,
) -> Vec<NivelSR> {
    buckets.iter().take(3).enumerate().map(|(i, (_, b))| {
        let score = if es_soporte { b.score_soporte() } else { b.score_resistencia() };
        NivelSR {
            precio_nivel: b.precio_centro,
            volumen_usdt: if es_soporte { b.vol_buy } else { b.vol_sell },
            trade_count: b.trades,
            buy_pct: b.pct_buy(),
            sell_pct: b.pct_sell(),
            seg_dominante: b.seg_dominante(),
            score,
            criterio: criterio.to_string(),
            tipo: tipo.to_string(),
            rank: (i + 1) as u8,
        }
    }).collect()
}

fn segmento_desde_vol(vol: f64) -> u8 {
    if vol < 40.0 { 1 }
    else if vol < 100.0 { 2 }
    else if vol < 500.0 { 3 }
    else if vol < 1_000.0 { 4 }
    else if vol < 2_500.0 { 5 }
    else if vol < 5_000.0 { 6 }
    else if vol < 10_000.0 { 7 }
    else if vol < 50_000.0 { 8 }
    else if vol < 100_000.0 { 9 }
    else if vol < 500_000.0 { 10 }
    else if vol < 1_000_000.0 { 11 }
    else if vol < 5_000_000.0 { 12 }
    else if vol < 10_000_000.0 { 13 }
    else { 14 }
}

fn escribir_batch(batch: &RecordBatch, schema: &Arc<Schema>, ruta: &str) -> Result<(), String> {
    let file = fs::File::create(ruta).map_err(|e| e.to_string())?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None)
        .map_err(|e| e.to_string())?;
    writer.write(batch).map_err(|e| e.to_string())?;
    writer.close().map_err(|e| e.to_string())?;
    Ok(())
}

fn guardar_df_como_parquet(df: DataFrame, ruta: &str) -> Result<(), String> {
    let file = fs::File::create(ruta).map_err(|e| e.to_string())?;
    ParquetWriter::new(file)
        .finish(&mut df.clone())
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn df_a_resultado(df: &DataFrame, timestamp: u64) -> ResultadoSR {
    let get_f64 = |col: &str, i: usize| -> f64 {
        df.column(col).ok().and_then(|c| c.f64().ok()).and_then(|a| a.get(i)).unwrap_or(0.0)
    };
    let get_u64 = |col: &str, i: usize| -> u64 {
        df.column(col).ok().and_then(|c| c.u64().ok()).and_then(|a| a.get(i)).unwrap_or(0)
    };
    let get_u8 = |col: &str, i: usize| -> u8 {
        df.column(col).ok().and_then(|c| c.u8().ok()).and_then(|a| a.get(i)).unwrap_or(0)
    };
    let get_str = |col: &str, i: usize| -> String {
        df.column(col).ok().and_then(|c| c.str().ok()).and_then(|a| a.get(i)).unwrap_or("").to_string()
    };

    let mut resultado = ResultadoSR {
        timestamp,
        sesion: get_str("sesion", 0),
        par: get_str("par", 0),
        precio_ref: get_f64("precio_ref", 0),
        bucket_size: get_f64("bucket_size", 0),
        ..Default::default()
    };

    for i in 0..df.height() {
        let nivel = NivelSR {
            precio_nivel: get_f64("precio_nivel", i),
            volumen_usdt: get_f64("volumen_usdt", i),
            trade_count: get_u64("trade_count", i),
            buy_pct: get_f64("buy_pct", i),
            sell_pct: get_f64("sell_pct", i),
            seg_dominante: get_u8("seg_dominante", i),
            score: get_f64("score", i),
            criterio: get_str("criterio", i),
            tipo: get_str("tipo", i),
            rank: get_u8("rank", i),
        };

        match (nivel.tipo.as_str(), nivel.criterio.as_str()) {
            ("soporte", "volumen") => resultado.soportes_vol.push(nivel),
            ("soporte", "trades") => resultado.soportes_trades.push(nivel),
            ("resistencia", "volumen") => resultado.resistencias_vol.push(nivel),
            ("resistencia", "trades") => resultado.resistencias_trades.push(nivel),
            _ => {}
        }
    }

    resultado
}

// ───────────────────────────────────────────────────────
// Scheduler: 15 minutos después de cada volcado
// ───────────────────────────────────────────────────────

use crate::config::control::segundos_hasta_proximo_volcado;
use std::sync::Arc as SyncArc;
use tokio::sync::RwLock;

pub async fn iniciar_scheduler_sr(
    par: SyncArc<RwLock<String>>,
    data_dir: String,
    ultimo_resultado: SyncArc<RwLock<Option<ResultadoSR>>>,
) {
    tracing::info!("SR SCHEDULER | Iniciado. Corre 15min después de cada volcado.");

    loop {
        // Esperar al próximo volcado + 15 minutos
        let espera = segundos_hasta_proximo_volcado() + 900; // 900s = 15min
        tracing::info!("SR SCHEDULER | Próximo análisis en {}h {}m",
            espera / 3600, (espera % 3600) / 60);

        tokio::time::sleep(tokio::time::Duration::from_secs(espera)).await;

        let par_actual = par.read().await.clone();
        tracing::info!("SR SCHEDULER | Ejecutando análisis S/R para {}", par_actual);

        // Correr análisis en thread blocking (Polars es sync)
        let motor = MotorSR::new(&data_dir);
        let par_clone = par_actual.clone();
        let resultado = tokio::task::spawn_blocking(move || {
            motor.analizar(&par_clone, "spot")
        }).await.unwrap_or_else(|e| ResultadoSR {
            error: Some(format!("Error en spawn_blocking: {}", e)),
            ..Default::default()
        });

        if let Some(ref err) = resultado.error {
            tracing::error!("SR SCHEDULER | Error: {}", err);
        } else {
            tracing::info!("SR SCHEDULER | Análisis completado: {} S + {} R (vol) | {} S + {} R (trades)",
                resultado.soportes_vol.len(), resultado.resistencias_vol.len(),
                resultado.soportes_trades.len(), resultado.resistencias_trades.len());
        }

        *ultimo_resultado.write().await = Some(resultado);
    }
}
