// ═══════════════════════════════════════════════════════
// FORJA v1.0 - M10: SQL Backstage con Polars Lazy
// ═══════════════════════════════════════════════════════
// Lee Parquet con Polars LazyFrame.
// Análisis predefinidos + exportación CSV.
// Sin DuckDB, sin bindings externos. Rust puro.
// ═══════════════════════════════════════════════════════

use polars::prelude::*;
use serde::Serialize;
use std::ops::Neg;
use std::path::Path;

// ───────────────────────────────────────────────────────
// Análisis predefinidos
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct AnalisisPredefinido {
    pub id: &'static str,
    pub nombre: &'static str,
    pub descripcion: &'static str,
}

pub const ANALISIS_PREDEFINIDOS: &[AnalisisPredefinido] = &[
    AnalisisPredefinido { id: "resumen", nombre: "Resumen del período", descripcion: "Estadísticas generales: trades, volumen, precio min/max, buy/sell %" },
    AnalisisPredefinido { id: "por_segmento", nombre: "Volumen por segmento P1-P14", descripcion: "Desglose de actividad por actor usando campo seg" },
    AnalisisPredefinido { id: "smart_vs_retail", nombre: "Smart Money vs Retail", descripcion: "Comparar dirección de P1-P4 (retail) vs P8-P14 (smart money)" },
    AnalisisPredefinido { id: "top_trades", nombre: "Top 20 trades más grandes", descripcion: "Los 20 trades con mayor volumen USDT" },
    AnalisisPredefinido { id: "por_hora", nombre: "Actividad por hora UTC", descripcion: "Volumen y buy/sell agrupado por hora del día" },
    AnalisisPredefinido { id: "delta_minuto", nombre: "Delta por minuto", descripcion: "Buy - Sell acumulado por minuto" },
    AnalisisPredefinido { id: "ballenas", nombre: "Movimientos de ballenas P10+", descripcion: "Trades institucionales y ballenas (seg >= 10)" },
    AnalisisPredefinido { id: "divergencia", nombre: "Divergencia Smart vs Retail", descripcion: "Minutos donde retail y smart money van en direcciones opuestas" },
    AnalisisPredefinido { id: "seg_semana", nombre: "Volumen por segmento × semana", descripcion: "Actividad semanal por actor — detecta ciclos de 4 semanas" },
    AnalisisPredefinido { id: "sr_diario", nombre: "S/R diario (Vol + Trades)", descripcion: "Top 3 soportes/resistencias por día: volumen buy/sell y trades" },
    AnalisisPredefinido { id: "sr_por_segmento", nombre: "S/R por segmento (psicología)", descripcion: "Donde cada actor defiende o abandona el precio — soporte/resistencia por actor" },
];

// ───────────────────────────────────────────────────────
// Resultado de análisis
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Default)]
pub struct AnalisisResult {
    pub columnas: Vec<String>,
    pub filas: Vec<Vec<String>>,
    pub total_filas: usize,
    pub tiempo_ms: u64,
    pub error: Option<String>,
}

// ───────────────────────────────────────────────────────
// Motor de análisis con Polars Lazy
// ───────────────────────────────────────────────────────

pub struct MotorAnalisis {
    data_dir: String,
}

impl MotorAnalisis {
    pub fn new(data_dir: &str) -> Self {
        Self { data_dir: data_dir.to_string() }
    }

    /// Ejecuta un análisis predefinido
    /// archivo: "all" = todos los parquets, o nombre específico ej "2026-04-30_12-00.parquet"
    pub fn ejecutar(&self, id: &str, par: &str, mercado: &str, archivo: &str) -> AnalisisResult {
        let inicio = std::time::Instant::now();

        let dir = format!("{}/{}/{}", self.data_dir, par, mercado);
        let ruta_scan = if archivo == "all" || archivo.is_empty() {
            format!("{}/*.parquet", dir)
        } else {
            format!("{}/{}", dir, archivo)
        };

        // Verificar que existe
        let archivos = self.listar_parquets(&dir);
        if archivos.is_empty() {
            return AnalisisResult {
                error: Some(format!("No hay archivos Parquet en {}", dir)),
                tiempo_ms: inicio.elapsed().as_millis() as u64,
                ..Default::default()
            };
        }

        // Cargar Parquet como LazyFrame
        let lf = if let Some(lista) = archivo.strip_prefix("rango:") {
            let rutas: Vec<std::path::PathBuf> = lista.split(',')
                .map(|f| std::path::PathBuf::from(format!("{}/{}", dir, f.trim())))
                .filter(|r| r.exists())
                .collect();
            if rutas.is_empty() {
                return AnalisisResult {
                    error: Some("No se encontraron archivos en el rango".to_string()),
                    tiempo_ms: inicio.elapsed().as_millis() as u64,
                    ..Default::default()
                };
            }
            match LazyFrame::scan_parquet_files(rutas.into(), ScanArgsParquet::default()) {
                Ok(lf) => lf,
                Err(e) => return AnalisisResult {
                    error: Some(format!("Error leyendo rango: {}", e)),
                    tiempo_ms: inicio.elapsed().as_millis() as u64,
                    ..Default::default()
                },
            }
        } else {
            match LazyFrame::scan_parquet(&ruta_scan, ScanArgsParquet::default()) {
                Ok(lf) => lf,
                Err(e) => return AnalisisResult {
                    error: Some(format!("Error leyendo Parquet '{}': {}", ruta_scan, e)),
                    tiempo_ms: inicio.elapsed().as_millis() as u64,
                    ..Default::default()
                },
            }
        };

        // Ejecutar análisis según el id
        let result_lf = match id {
            "resumen" => self.analisis_resumen(lf),
            "por_segmento" => self.analisis_por_segmento(lf),
            "smart_vs_retail" => self.analisis_smart_vs_retail(lf),
            "top_trades" => self.analisis_top_trades(lf),
            "por_hora" => self.analisis_por_hora(lf),
            "delta_minuto" => self.analisis_delta_minuto(lf),
            "ballenas" => self.analisis_ballenas(lf),
            "divergencia"    => self.analisis_divergencia(lf),
            "seg_semana"     => self.analisis_seg_semana(lf),
            "sr_diario"      => self.analisis_sr_diario(lf),
            "sr_por_segmento"=> self.analisis_sr_por_segmento(lf),
            _ => return AnalisisResult {
                error: Some(format!("Análisis '{}' no encontrado", id)),
                tiempo_ms: inicio.elapsed().as_millis() as u64,
                ..Default::default()
            },
        };

        match result_lf {
            Ok(lf) => self.collect_result(lf, inicio),
            Err(e) => AnalisisResult {
                error: Some(format!("Error en análisis: {}", e)),
                tiempo_ms: inicio.elapsed().as_millis() as u64,
                ..Default::default()
            },
        }
    }

    // ───────────────────────────────────────────────────
    // Análisis predefinidos con Polars Lazy
    // ───────────────────────────────────────────────────

    fn analisis_resumen(&self, lf: LazyFrame) -> Result<LazyFrame, PolarsError> {
        Ok(lf.group_by([lit(1i32).alias("_g")])
            .agg([
                len().alias("trades"),
                col("volume_usdt").sum().round(2).alias("vol_total_usdt"),
                col("price").mean().round(2).alias("precio_avg"),
                col("price").min().round(2).alias("precio_min"),
                col("price").max().round(2).alias("precio_max"),
                (when(col("side").eq(lit(0u8))).then(col("volume_usdt")).otherwise(lit(0.0f64)).sum()
                    / col("volume_usdt").sum() * lit(100.0f64)).round(1).alias("buy_pct"),
                (when(col("side").eq(lit(1u8))).then(col("volume_usdt")).otherwise(lit(0.0f64)).sum()
                    / col("volume_usdt").sum() * lit(100.0f64)).round(1).alias("sell_pct"),
            ])
            .select([col("trades"),col("vol_total_usdt"),col("precio_avg"),
                     col("precio_min"),col("precio_max"),col("buy_pct"),col("sell_pct")]))
    }

    fn analisis_por_segmento(&self, lf: LazyFrame) -> Result<LazyFrame, PolarsError> {
        let total = lf.clone().select([col("volume_usdt").sum().alias("total")]).collect()?;
        let total_vol: f64 = total.column("total")?.f64()?.get(0).unwrap_or(1.0);

        Ok(lf.group_by([col("seg")])
            .agg([
                col("volume_usdt").count().alias("trades"),
                col("volume_usdt").sum().round(2).alias("vol_usdt"),
                (col("volume_usdt").sum() / lit(total_vol) * lit(100.0f64)).round(2).alias("pct_vol"),
                (when(col("side").eq(lit(0u8))).then(col("volume_usdt")).otherwise(lit(0.0f64)).sum()
                    / col("volume_usdt").sum() * lit(100.0f64)).round(1).alias("buy_pct"),
                (when(col("side").eq(lit(1u8))).then(col("volume_usdt")).otherwise(lit(0.0f64)).sum()
                    / col("volume_usdt").sum() * lit(100.0f64)).round(1).alias("sell_pct"),
            ])
            .sort(["seg"], SortMultipleOptions::default()))
    }

    fn analisis_smart_vs_retail(&self, lf: LazyFrame) -> Result<LazyFrame, PolarsError> {
        Ok(lf.with_column(
            when(col("seg").lt_eq(lit(4u8))).then(lit("Retail (P1-P4)"))
                .when(col("seg").lt_eq(lit(7u8))).then(lit("Bisagra (P5-P7)"))
                .otherwise(lit("Smart Money (P8-P14)"))
                .alias("grupo")
        )
        .group_by([col("grupo")])
        .agg([
            col("volume_usdt").count().alias("trades"),
            col("volume_usdt").sum().round(2).alias("vol_usdt"),
            (when(col("side").eq(lit(0u8))).then(col("volume_usdt")).otherwise(lit(0.0f64)).sum()
                / col("volume_usdt").sum() * lit(100.0f64)).round(1).alias("buy_pct"),
            (when(col("side").eq(lit(1u8))).then(col("volume_usdt")).otherwise(lit(0.0f64)).sum()
                / col("volume_usdt").sum() * lit(100.0f64)).round(1).alias("sell_pct"),
        ])
        .sort(["grupo"], SortMultipleOptions::default()))
    }

    fn analisis_top_trades(&self, lf: LazyFrame) -> Result<LazyFrame, PolarsError> {
        Ok(lf.select([
            col("timestamp"),
            col("price").round(2).alias("precio"),
            col("volume_usdt").round(2).alias("usdt"),
            col("volume_base").round(6).alias("base"),
            col("side"),
            col("seg"),
        ])
        .sort(["usdt"], SortMultipleOptions::new().with_order_descending(true))
        .limit(20))
    }

    fn analisis_por_hora(&self, lf: LazyFrame) -> Result<LazyFrame, PolarsError> {
        Ok(lf.with_column(
            // timestamp en ms → hora UTC sin cast a Datetime
            (col("timestamp") / lit(3_600_000u64) % lit(24u64))
                .cast(DataType::Int32).alias("hora_utc")
        )
        .group_by([col("hora_utc")])
        .agg([
            col("volume_usdt").count().alias("trades"),
            col("volume_usdt").sum().round(2).alias("vol_usdt"),
            (when(col("side").eq(lit(0u8))).then(col("volume_usdt")).otherwise(lit(0.0f64)).sum()
                / col("volume_usdt").sum() * lit(100.0f64)).round(1).alias("buy_pct"),
            (when(col("side").eq(lit(1u8))).then(col("volume_usdt")).otherwise(lit(0.0f64)).sum()
                / col("volume_usdt").sum() * lit(100.0f64)).round(1).alias("sell_pct"),
        ])
        .sort(["hora_utc"], SortMultipleOptions::default()))
    }

    fn analisis_delta_minuto(&self, lf: LazyFrame) -> Result<LazyFrame, PolarsError> {
        Ok(lf.with_column(
            (col("timestamp") / lit(60_000u64) * lit(60_000u64)).alias("minuto")
        )
        .group_by([col("minuto")])
        .agg([
            col("volume_usdt").count().alias("trades"),
            when(col("side").eq(lit(0u8))).then(col("volume_usdt")).otherwise(lit(0.0f64))
                .sum().round(2).alias("buy_vol"),
            when(col("side").eq(lit(1u8))).then(col("volume_usdt")).otherwise(lit(0.0f64))
                .sum().round(2).alias("sell_vol"),
            (when(col("side").eq(lit(0u8))).then(col("volume_usdt")).otherwise(col("volume_usdt").neg())
                .sum().round(2)).alias("delta"),
        ])
        .sort(["minuto"], SortMultipleOptions::default()))
    }

    fn analisis_ballenas(&self, lf: LazyFrame) -> Result<LazyFrame, PolarsError> {
        Ok(lf
            .filter(col("seg").cast(DataType::UInt8).gt_eq(lit(10u8)))
            .with_column(
                (col("timestamp").cast(DataType::Int64) / lit(86_400_000i64))
                    .cast(DataType::Int32).cast(DataType::Date).cast(DataType::String)
                    .alias("fecha")
            )
            .select([
                col("fecha"),
                col("price").round(2).alias("precio"),
                col("volume_usdt").round(2).alias("usdt"),
                when(col("side").eq(lit(0u8))).then(lit("BUY")).otherwise(lit("SELL")).alias("side"),
                col("seg"),
            ])
            .sort(["usdt"], SortMultipleOptions::new().with_order_descending(true))
            .limit(50))
    }

    fn analisis_divergencia(&self, lf: LazyFrame) -> Result<LazyFrame, PolarsError> {
        Ok(lf
        .with_column(
            col("timestamp").cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                .dt().truncate("1m".into()).alias("minuto")
        )
        .group_by([col("minuto")])
        .agg([
            // Retail P1-P4: fill_null(50) cuando no hay trades en el minuto
            (when(col("seg").lt_eq(lit(4u8)).and(col("side").eq(lit(0u8))))
                .then(col("volume_usdt")).otherwise(lit(0.0f64)).sum()
                / (when(col("seg").lt_eq(lit(4u8)))
                    .then(col("volume_usdt")).otherwise(lit(0.0f64)).sum()
                    + lit(1e-9f64))
                * lit(100.0f64)).round(1).alias("retail_buy_pct"),
            // Smart money P8-P14
            (when(col("seg").gt_eq(lit(8u8)).and(col("side").eq(lit(0u8))))
                .then(col("volume_usdt")).otherwise(lit(0.0f64)).sum()
                / (when(col("seg").gt_eq(lit(8u8)))
                    .then(col("volume_usdt")).otherwise(lit(0.0f64)).sum()
                    + lit(1e-9f64))
                * lit(100.0f64)).round(1).alias("smart_buy_pct_raw"),
            // Flag: ¿hubo smart money en este minuto?
            when(col("seg").gt_eq(lit(8u8)))
                .then(col("volume_usdt")).otherwise(lit(0.0f64))
                .sum().alias("smart_vol"),
        ])
        .with_columns([
            // Si no hubo smart money → mostrar "-" en vez de 0
            when(col("smart_vol").gt(lit(0.0f64)))
                .then(col("smart_buy_pct_raw"))
                .otherwise(lit(f64::NAN))
                .alias("smart_buy_pct"),
        ])
        .with_column(
            when(col("smart_vol").gt(lit(0.0f64)))
                .then((col("smart_buy_pct_raw") - col("retail_buy_pct")).round(1))
                .otherwise(lit(f64::NAN))
                .alias("divergencia")
        )
        .select([col("minuto"), col("retail_buy_pct"), col("smart_buy_pct"), col("divergencia")])
        .sort(["minuto"], SortMultipleOptions::default()))
    }


    fn analisis_seg_semana(&self, lf: LazyFrame) -> Result<LazyFrame, PolarsError> {
        // Convertir timestamp ms → fecha → año y semana ISO
        Ok(lf.with_columns([
            // Año UTC
            col("timestamp").cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                .dt().year().alias("anio"),
            // Semana ISO (1-53)
            col("timestamp").cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                .dt().week().alias("semana_num"),
        ])
        .with_column(
            // Formato "2026-W18" — concatenación manual compatible con polars 0.46
            (col("anio").cast(DataType::String)
                + lit("-W")
                + col("semana_num").cast(DataType::String))
                .alias("semana")
        )
        .group_by([col("semana"), col("seg")])
        .agg([
            len().alias("trades"),
            col("volume_usdt").sum().round(2).alias("vol_usdt"),
            (when(col("side").eq(lit(0u8))).then(col("volume_usdt")).otherwise(lit(0.0f64)).sum()
                / col("volume_usdt").sum() * lit(100.0f64)).round(1).alias("buy_pct"),
            (when(col("side").eq(lit(1u8))).then(col("volume_usdt")).otherwise(lit(0.0f64)).sum()
                / col("volume_usdt").sum() * lit(100.0f64)).round(1).alias("sell_pct"),
        ])
        .sort(["semana", "seg"], SortMultipleOptions::default()))
    }

    fn analisis_sr_diario(&self, lf: LazyFrame) -> Result<LazyFrame, PolarsError> {
        let escalon = 0.50f64;
        Ok(lf
            .with_columns([
                // Día como fecha legible
                col("timestamp").cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                    .dt().date().cast(DataType::String).alias("fecha"),
                // Centro del bucket (escalón $0.50)
                ((col("price") / lit(escalon)).floor() * lit(escalon) + lit(escalon / 2.0f64))
                    .round(2).alias("precio_nivel"),
            ])
            .group_by([col("fecha"), col("precio_nivel")])
            .agg([
                len().alias("trades"),
                col("volume_usdt").sum().round(2).alias("vol_total"),
                when(col("side").eq(lit(0u8))).then(col("volume_usdt")).otherwise(lit(0.0f64))
                    .sum().round(2).alias("vol_buy"),
                when(col("side").eq(lit(1u8))).then(col("volume_usdt")).otherwise(lit(0.0f64))
                    .sum().round(2).alias("vol_sell"),
                (when(col("side").eq(lit(0u8))).then(col("volume_usdt")).otherwise(lit(0.0f64)).sum()
                    / col("volume_usdt").sum() * lit(100.0f64)).round(1).alias("buy_pct"),
                col("seg").first().alias("seg_dom"),
            ])
            // Clasificar soporte/resistencia según dominancia buy/sell
            .with_column(
                when(col("buy_pct").gt_eq(lit(52.0f64)))
                    .then(lit("SOPORTE"))
                    .when(col("buy_pct").lt_eq(lit(48.0f64)))
                    .then(lit("RESISTENCIA"))
                    .otherwise(lit("NEUTRAL"))
                    .alias("rol")
            )
            .select([
                col("fecha"), col("precio_nivel"), col("rol"),
                col("trades"), col("vol_total"), col("vol_buy"), col("vol_sell"),
                col("buy_pct"), col("seg_dom"),
            ])
            .sort(["fecha", "vol_total"], SortMultipleOptions::new().with_order_descending_multi([false, true]))
        )
    }

    fn analisis_sr_por_segmento(&self, lf: LazyFrame) -> Result<LazyFrame, PolarsError> {
        let escalon = 0.50f64;
        Ok(lf
            .with_column(
                ((col("price") / lit(escalon)).floor() * lit(escalon) + lit(escalon / 2.0f64))
                    .round(2).alias("precio_nivel")
            )
            .group_by([col("seg"), col("precio_nivel")])
            .agg([
                len().alias("trades"),
                col("volume_usdt").sum().round(2).alias("vol_total"),
                when(col("side").eq(lit(0u8))).then(col("volume_usdt")).otherwise(lit(0.0f64))
                    .sum().round(2).alias("vol_buy"),
                when(col("side").eq(lit(1u8))).then(col("volume_usdt")).otherwise(lit(0.0f64))
                    .sum().round(2).alias("vol_sell"),
                (when(col("side").eq(lit(0u8))).then(col("volume_usdt")).otherwise(lit(0.0f64)).sum()
                    / col("volume_usdt").sum() * lit(100.0f64)).round(1).alias("buy_pct"),
                (when(col("side").eq(lit(1u8))).then(col("volume_usdt")).otherwise(lit(0.0f64)).sum()
                    / col("volume_usdt").sum() * lit(100.0f64)).round(1).alias("sell_pct"),
            ])
            // Clasificar rol por dominancia en ese nivel
            .with_column(
                when(col("buy_pct").gt_eq(lit(52.0f64)))
                    .then(lit("SOPORTE"))
                    .when(col("buy_pct").lt_eq(lit(48.0f64)))
                    .then(lit("RESISTENCIA"))
                    .otherwise(lit("NEUTRAL"))
                    .alias("rol")
            )
            .select([
                col("seg"), col("precio_nivel"), col("rol"),
                col("trades"), col("vol_total"), col("vol_buy"), col("vol_sell"),
                col("buy_pct"), col("sell_pct"),
            ])
            // Ordenar por seg asc, vol_total desc
            .sort(["seg", "vol_total"], SortMultipleOptions::new().with_order_descending_multi([false, true]))
        )
    }

    // ───────────────────────────────────────────────────
    // Recolectar DataFrame a AnalisisResult
    // ───────────────────────────────────────────────────

    fn collect_result(&self, lf: LazyFrame, inicio: std::time::Instant) -> AnalisisResult {
        match lf.collect() {
            Ok(df) => {
                let columnas: Vec<String> = df.get_column_names()
                    .iter().map(|s| s.to_string()).collect();
                let nrows = df.height();
                let mut filas: Vec<Vec<String>> = Vec::with_capacity(nrows);

                for row in 0..nrows {
                    let mut fila = Vec::with_capacity(columnas.len());
                    for col_name in &columnas {
                        let val = df.column(col_name)
                            .map(|s| series_val(s, row))
                            .unwrap_or_else(|_| "NULL".to_string());
                        fila.push(val);
                    }
                    filas.push(fila);
                }

                AnalisisResult {
                    columnas, total_filas: nrows, filas,
                    tiempo_ms: inicio.elapsed().as_millis() as u64,
                    error: None,
                }
            }
            Err(e) => AnalisisResult {
                error: Some(format!("Error recolectando: {}", e)),
                tiempo_ms: inicio.elapsed().as_millis() as u64,
                ..Default::default()
            },
        }
    }

    /// Lista archivos Parquet disponibles
    pub fn listar_parquets(&self, dir: &str) -> Vec<String> {
        let path = Path::new(dir);
        if !path.is_dir() { return Vec::new(); }
        let mut archivos = Vec::new();
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.extension().map(|e| e == "parquet").unwrap_or(false) {
                    archivos.push(p.to_string_lossy().to_string());
                }
            }
        }
        archivos.sort();
        archivos
    }

    /// Lista pares disponibles
    pub fn listar_pares(&self) -> Vec<String> {
        let path = Path::new(&self.data_dir);
        if !path.is_dir() { return Vec::new(); }
        let mut pares = Vec::new();
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                if entry.path().is_dir() {
                    pares.push(entry.file_name().to_string_lossy().to_string());
                }
            }
        }
        pares.sort();
        pares
    }

    /// Exporta resultado a CSV
    pub fn exportar_csv(result: &AnalisisResult) -> String {
        let mut csv = result.columnas.join(",") + "\n";
        for fila in &result.filas {
            csv += &fila.iter()
                .map(|v| if v.contains(',') { format!("\"{}\"", v) } else { v.clone() })
                .collect::<Vec<_>>()
                .join(",");
            csv += "\n";
        }
        csv
    }
}

// ───────────────────────────────────────────────────────
// Info de archivos Parquet
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct ParquetInfo {
    pub nombre: String,
    pub size_kb: u64,
}

pub fn listar_parquets_info(dir: &str) -> Vec<ParquetInfo> {
    let path = Path::new(dir);
    if !path.is_dir() { return Vec::new(); }
    let mut archivos = Vec::new();
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.extension().map(|e| e == "parquet").unwrap_or(false) {
                let size_kb = std::fs::metadata(&p).map(|m| m.len() / 1024).unwrap_or(0);
                archivos.push(ParquetInfo {
                    nombre: p.file_name().unwrap_or_default().to_string_lossy().to_string(),
                    size_kb,
                });
            }
        }
    }
    archivos.sort_by(|a, b| a.nombre.cmp(&b.nombre));
    archivos
}

// ───────────────────────────────────────────────────────
// Helper para extraer valor de Series
// ───────────────────────────────────────────────────────

fn series_val(s: &Column, row: usize) -> String {
    let s = s.as_series();
    let s = match s {
        Some(s) => s,
        None => return "NULL".to_string(),
    };
    match s.dtype() {
        DataType::Float64 => s.f64().ok().and_then(|a| a.get(row)).map(|v| format!("{:.4}", v)).unwrap_or_else(|| "NULL".to_string()),
        DataType::Float32 => s.f32().ok().and_then(|a| a.get(row)).map(|v| format!("{:.4}", v)).unwrap_or_else(|| "NULL".to_string()),
        DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 =>
            s.cast(&DataType::Int64).ok().and_then(|s| s.i64().ok().and_then(|a| a.get(row)).map(|v| v.to_string())).unwrap_or_else(|| "NULL".to_string()),
        DataType::UInt64 | DataType::UInt32 | DataType::UInt16 | DataType::UInt8 =>
            s.cast(&DataType::UInt64).ok().and_then(|s| s.u64().ok().and_then(|a| a.get(row)).map(|v| v.to_string())).unwrap_or_else(|| "NULL".to_string()),
        DataType::Boolean => s.bool().ok().and_then(|a| a.get(row)).map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()),
        DataType::String => s.str().ok().and_then(|a| a.get(row)).map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()),
        _ => format!("{}", s.get(row).unwrap_or(AnyValue::Null)),
    }
}
