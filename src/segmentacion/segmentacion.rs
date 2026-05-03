// ═══════════════════════════════════════════════════════
// FORJA v1.0 - Motor de Segmentación
// ═══════════════════════════════════════════════════════
// Clasifica cada trade por volumen USDT en uno de los
// 14 segmentos de actor. Los rangos son configurables
// por par para adaptarse a la liquidez de cada activo.
//
// La clasificación es un encadenamiento de comparaciones
// sobre f64: costo CPU prácticamente cero incluso
// procesando miles de trades por segundo.
// ═══════════════════════════════════════════════════════

use crate::normalizador::normalizador::TradeFORJA;

// ───────────────────────────────────────────────────────
// Constantes
// ───────────────────────────────────────────────────────

/// Cantidad total de segmentos
pub const TOTAL_SEGMENTOS: usize = 14;

/// Nombres cortos para UI (ahorro de espacio en pantalla)
pub const NOMBRES_CORTOS: [&str; TOTAL_SEGMENTOS] = [
    "P1", "P2", "P3", "P4", "P5", "P6", "P7",
    "P8", "P9", "P10", "P11", "P12", "P13", "P14",
];

/// Nombres completos para reportes y Parquet
pub const NOMBRES_COMPLETOS: [&str; TOTAL_SEGMENTOS] = [
    "micro_retail",
    "retail_puro",
    "retail_pequeno",
    "retail_activo",
    "retail_plus",
    "bisagra_bajo",
    "bisagra_alto",
    "profesional",
    "institucional_bajo",
    "institucional_alto",
    "ballena_baby",
    "ballena_azul",
    "whale_evento",
    "market_maker",
];

/// Rangos por defecto (valor máximo de cada segmento en USDT)
/// El mínimo se hereda del máximo del segmento anterior.
/// P14 (market_maker) no tiene techo.
pub const RANGOS_DEFAULT: [f64; TOTAL_SEGMENTOS - 1] = [
    40.0,        // P1  micro_retail:      0 - 39.99
    100.0,       // P2  retail_puro:       40 - 99.99
    500.0,       // P3  retail_pequeno:    100 - 499.99
    1_000.0,     // P4  retail_activo:     500 - 999.99
    2_500.0,     // P5  retail_plus:       1,000 - 2,499.99
    5_000.0,     // P6  bisagra_bajo:      2,500 - 4,999.99
    10_000.0,    // P7  bisagra_alto:      5,000 - 9,999.99
    50_000.0,    // P8  profesional:       10,000 - 49,999.99
    100_000.0,   // P9  institucional_bajo: 50,000 - 99,999.99
    500_000.0,   // P10 institucional_alto: 100,000 - 499,999.99
    1_000_000.0, // P11 ballena_baby:      500,000 - 999,999.99
    5_000_000.0, // P12 ballena_azul:      1,000,000 - 4,999,999.99
    10_000_000.0,// P13 whale_evento:      5,000,000 - 9,999,999.99
                 // P14 market_maker:      10,000,000+
];

// ───────────────────────────────────────────────────────
// Trade segmentado
// ───────────────────────────────────────────────────────

/// Trade FORJA con su segmento asignado.
/// Extiende TradeFORJA con el ID de segmento (1-14).
#[derive(Debug, Clone, Copy)]
pub struct TradeSegmentado {
    /// Trade original normalizado
    pub trade: TradeFORJA,
    /// ID del segmento (1-14)
    pub segment_id: u8,
}

impl TradeSegmentado {
    /// Nombre corto del segmento (P1-P14)
    pub fn nombre_corto(&self) -> &'static str {
        let idx = (self.segment_id as usize).saturating_sub(1);
        if idx < TOTAL_SEGMENTOS {
            NOMBRES_CORTOS[idx]
        } else {
            "P??"
        }
    }

    /// Nombre completo del segmento
    pub fn nombre_completo(&self) -> &'static str {
        let idx = (self.segment_id as usize).saturating_sub(1);
        if idx < TOTAL_SEGMENTOS {
            NOMBRES_COMPLETOS[idx]
        } else {
            "desconocido"
        }
    }
}

impl std::fmt::Display for TradeSegmentado {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} | {:.2} USDT | {} | {}",
            self.nombre_corto(),
            self.trade.volume_usdt,
            self.trade.side_str(),
            self.nombre_completo(),
        )
    }
}

// ───────────────────────────────────────────────────────
// Configuración de rangos por par
// ───────────────────────────────────────────────────────

/// Configuración de segmentación para un par específico.
/// Los rangos son los valores máximos de cada segmento.
/// 13 valores para 14 segmentos (el último no tiene techo).
#[derive(Debug, Clone)]
pub struct ConfigSegmentos {
    /// Nombre del par (ej: "BTCUSDT")
    pub par: String,
    /// Valores máximos de cada segmento (13 valores)
    /// rangos[0] = máximo P1, rangos[1] = máximo P2, etc.
    pub rangos: [f64; TOTAL_SEGMENTOS - 1],
}

impl ConfigSegmentos {
    /// Crea configuración con rangos por defecto
    pub fn default_para(par: &str) -> Self {
        Self {
            par: par.to_uppercase(),
            rangos: RANGOS_DEFAULT,
        }
    }

    /// Crea configuración con rangos personalizados
    pub fn new(par: &str, rangos: [f64; TOTAL_SEGMENTOS - 1]) -> Self {
        Self {
            par: par.to_uppercase(),
            rangos,
        }
    }

    /// Valida que los rangos estén en orden ascendente
    pub fn validar(&self) -> Result<(), String> {
        for i in 1..self.rangos.len() {
            if self.rangos[i] <= self.rangos[i - 1] {
                return Err(format!(
                    "Rango P{} ({}) debe ser mayor que P{} ({})",
                    i + 1,
                    self.rangos[i],
                    i,
                    self.rangos[i - 1]
                ));
            }
        }
        // Todos deben ser positivos
        if self.rangos[0] <= 0.0 {
            return Err("El rango mínimo (P1) debe ser mayor que 0".to_string());
        }
        Ok(())
    }

    /// Retorna el rango legible de un segmento
    pub fn rango_texto(&self, segment_id: u8) -> String {
        let idx = segment_id as usize;
        if idx == 0 || idx > TOTAL_SEGMENTOS {
            return "inválido".to_string();
        }

        let min = if idx == 1 {
            0.0
        } else {
            self.rangos[idx - 2]
        };

        if idx == TOTAL_SEGMENTOS {
            format!("{:.0}+", self.rangos[TOTAL_SEGMENTOS - 2])
        } else {
            format!("{:.0} - {:.0}", min, self.rangos[idx - 1])
        }
    }
}

impl std::fmt::Display for ConfigSegmentos {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Segmentación: {}", self.par)?;
        for i in 0..TOTAL_SEGMENTOS {
            let id = (i + 1) as u8;
            writeln!(
                f,
                "  {} {:<20} │ {} USDT",
                NOMBRES_CORTOS[i],
                NOMBRES_COMPLETOS[i],
                self.rango_texto(id),
            )?;
        }
        Ok(())
    }
}

// ───────────────────────────────────────────────────────
// Motor de Segmentación
// ───────────────────────────────────────────────────────

/// Motor que clasifica trades según la configuración de rangos.
/// Una instancia por par activo.
pub struct MotorSegmentacion {
    config: ConfigSegmentos,
}

impl MotorSegmentacion {
    /// Crea un motor con la configuración dada
    pub fn new(config: ConfigSegmentos) -> Result<Self, String> {
        config.validar()?;
        tracing::info!(
            "SEGMENTACION | Motor inicializado para {} con {} segmentos",
            config.par,
            TOTAL_SEGMENTOS
        );
        Ok(Self { config })
    }

    /// Crea un motor con rangos por defecto para un par
    pub fn default_para(par: &str) -> Self {
        Self {
            config: ConfigSegmentos::default_para(par),
        }
    }

    /// Clasifica un trade y retorna su segment_id (1-14).
    /// Operación O(1) prácticamente: máximo 13 comparaciones.
    #[inline]
    pub fn clasificar(&self, volume_usdt: f64) -> u8 {
        // Comparaciones encadenadas (de menor a mayor)
        // El compilador optimiza esto a una secuencia de
        // comparaciones sin branches innecesarios
        let rangos = &self.config.rangos;

        if volume_usdt < rangos[0] {
            1  // P1 micro_retail
        } else if volume_usdt < rangos[1] {
            2  // P2 retail_puro
        } else if volume_usdt < rangos[2] {
            3  // P3 retail_pequeno
        } else if volume_usdt < rangos[3] {
            4  // P4 retail_activo
        } else if volume_usdt < rangos[4] {
            5  // P5 retail_plus
        } else if volume_usdt < rangos[5] {
            6  // P6 bisagra_bajo
        } else if volume_usdt < rangos[6] {
            7  // P7 bisagra_alto
        } else if volume_usdt < rangos[7] {
            8  // P8 profesional
        } else if volume_usdt < rangos[8] {
            9  // P9 institucional_bajo
        } else if volume_usdt < rangos[9] {
            10 // P10 institucional_alto
        } else if volume_usdt < rangos[10] {
            11 // P11 ballena_baby
        } else if volume_usdt < rangos[11] {
            12 // P12 ballena_azul
        } else if volume_usdt < rangos[12] {
            13 // P13 whale_evento
        } else {
            14 // P14 market_maker
        }
    }

    /// Clasifica un trade y retorna un TradeSegmentado
    pub fn segmentar(&self, trade: TradeFORJA) -> TradeSegmentado {
        let segment_id = self.clasificar(trade.volume_usdt);
        TradeSegmentado { trade, segment_id }
    }

    /// Retorna la configuración actual
    pub fn config(&self) -> &ConfigSegmentos {
        &self.config
    }

    /// Actualiza los rangos (para ajustar sin recompilar)
    pub fn actualizar_rangos(&mut self, rangos: [f64; TOTAL_SEGMENTOS - 1]) -> Result<(), String> {
        let nueva_config = ConfigSegmentos::new(&self.config.par, rangos);
        nueva_config.validar()?;
        self.config = nueva_config;
        tracing::info!("SEGMENTACION | Rangos actualizados para {}", self.config.par);
        Ok(())
    }
}

impl std::fmt::Display for MotorSegmentacion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.config)
    }
}

// ───────────────────────────────────────────────────────
// Tests
// ───────────────────────────────────────────────────────


// ───────────────────────────────────────────────────────
// Clasificación de segmento por volumen USDT
// Fuente única de verdad — usar desde sr.rs y analisis.rs
// ───────────────────────────────────────────────────────

/// Clasifica un trade en segmento P1-P14 según su volumen USDT.
/// Usa los mismos rangos que RANGOS_DEFAULT.
pub fn seg_desde_vol(vol: f64) -> u8 {
    if      vol <        40.0 { 1  }
    else if vol <       100.0 { 2  }
    else if vol <       500.0 { 3  }
    else if vol <     1_000.0 { 4  }
    else if vol <     2_500.0 { 5  }
    else if vol <     5_000.0 { 6  }
    else if vol <    10_000.0 { 7  }
    else if vol <    50_000.0 { 8  }
    else if vol <   100_000.0 { 9  }
    else if vol <   500_000.0 { 10 }
    else if vol < 1_000_000.0 { 11 }
    else if vol < 5_000_000.0 { 12 }
    else if vol <10_000_000.0 { 13 }
    else                      { 14 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::normalizador::normalizador::SIDE_BUY;

    fn trade_con_volumen(vol_usdt: f64) -> TradeFORJA {
        TradeFORJA::new(1712678400000, 68000.0, vol_usdt, vol_usdt / 68000.0, SIDE_BUY)
    }

    #[test]
    fn test_clasificar_todos_los_segmentos() {
        let motor = MotorSegmentacion::default_para("BTCUSDT");

        // P1: 0 - 39.99
        assert_eq!(motor.clasificar(0.01), 1);
        assert_eq!(motor.clasificar(10.0), 1);
        assert_eq!(motor.clasificar(39.99), 1);

        // P2: 40 - 99.99
        assert_eq!(motor.clasificar(40.0), 2);
        assert_eq!(motor.clasificar(99.99), 2);

        // P3: 100 - 499.99
        assert_eq!(motor.clasificar(100.0), 3);
        assert_eq!(motor.clasificar(499.99), 3);

        // P4: 500 - 999.99
        assert_eq!(motor.clasificar(500.0), 4);
        assert_eq!(motor.clasificar(999.99), 4);

        // P5: 1,000 - 2,499.99
        assert_eq!(motor.clasificar(1000.0), 5);
        assert_eq!(motor.clasificar(2499.99), 5);

        // P6: 2,500 - 4,999.99
        assert_eq!(motor.clasificar(2500.0), 6);
        assert_eq!(motor.clasificar(4999.99), 6);

        // P7: 5,000 - 9,999.99
        assert_eq!(motor.clasificar(5000.0), 7);
        assert_eq!(motor.clasificar(9999.99), 7);

        // P8: 10,000 - 49,999.99
        assert_eq!(motor.clasificar(10000.0), 8);
        assert_eq!(motor.clasificar(49999.99), 8);

        // P9: 50,000 - 99,999.99
        assert_eq!(motor.clasificar(50000.0), 9);
        assert_eq!(motor.clasificar(99999.99), 9);

        // P10: 100,000 - 499,999.99
        assert_eq!(motor.clasificar(100000.0), 10);
        assert_eq!(motor.clasificar(499999.99), 10);

        // P11: 500,000 - 999,999.99
        assert_eq!(motor.clasificar(500000.0), 11);
        assert_eq!(motor.clasificar(999999.99), 11);

        // P12: 1,000,000 - 4,999,999.99
        assert_eq!(motor.clasificar(1000000.0), 12);
        assert_eq!(motor.clasificar(4999999.99), 12);

        // P13: 5,000,000 - 9,999,999.99
        assert_eq!(motor.clasificar(5000000.0), 13);
        assert_eq!(motor.clasificar(9999999.99), 13);

        // P14: 10,000,000+
        assert_eq!(motor.clasificar(10000000.0), 14);
        assert_eq!(motor.clasificar(999999999.0), 14);

        println!("✓ Todos los segmentos clasifican correctamente");
    }

    #[test]
    fn test_clasificar_limites_exactos() {
        let motor = MotorSegmentacion::default_para("BTCUSDT");

        // En el límite exacto debe caer en el segmento superior
        assert_eq!(motor.clasificar(40.0), 2);   // exacto en límite → P2
        assert_eq!(motor.clasificar(100.0), 3);   // exacto → P3
        assert_eq!(motor.clasificar(500.0), 4);   // exacto → P4
        assert_eq!(motor.clasificar(10000.0), 8);  // exacto → P8
        assert_eq!(motor.clasificar(10000000.0), 14); // exacto → P14

        println!("✓ Límites exactos correctos");
    }

    #[test]
    fn test_segmentar_trade() {
        let motor = MotorSegmentacion::default_para("BTCUSDT");

        let trade = trade_con_volumen(102.63); // ~P3 retail_pequeño
        let seg = motor.segmentar(trade);

        assert_eq!(seg.segment_id, 3);
        assert_eq!(seg.nombre_corto(), "P3");
        assert_eq!(seg.nombre_completo(), "retail_pequeno");

        println!("Trade {:.2} USDT → {}", seg.trade.volume_usdt, seg);
    }

    #[test]
    fn test_segmentar_trade_ballena() {
        let motor = MotorSegmentacion::default_para("BTCUSDT");

        let trade = trade_con_volumen(7_500_000.0); // whale_evento
        let seg = motor.segmentar(trade);

        assert_eq!(seg.segment_id, 13);
        assert_eq!(seg.nombre_corto(), "P13");
        assert_eq!(seg.nombre_completo(), "whale_evento");

        println!("Trade {:.2} USDT → {}", seg.trade.volume_usdt, seg);
    }

    #[test]
    fn test_segmentar_trade_market_maker() {
        let motor = MotorSegmentacion::default_para("BTCUSDT");

        let trade = trade_con_volumen(25_000_000.0); // market_maker
        let seg = motor.segmentar(trade);

        assert_eq!(seg.segment_id, 14);
        assert_eq!(seg.nombre_corto(), "P14");

        println!("Trade {:.2} USDT → {}", seg.trade.volume_usdt, seg);
    }

    #[test]
    fn test_config_personalizada() {
        // Rangos personalizados para un altcoin de menor capitalización
        let rangos = [
            10.0,       // P1: 0 - 9.99
            50.0,       // P2: 10 - 49.99
            200.0,      // P3: 50 - 199.99
            500.0,      // P4: 200 - 499.99
            1_000.0,    // P5
            2_000.0,    // P6
            5_000.0,    // P7
            10_000.0,   // P8
            50_000.0,   // P9
            100_000.0,  // P10
            250_000.0,  // P11
            500_000.0,  // P12
            1_000_000.0,// P13
        ];

        let config = ConfigSegmentos::new("WLDUSDT", rangos);
        assert!(config.validar().is_ok());

        let motor = MotorSegmentacion::new(config).unwrap();

        // Con rangos más bajos, 50 USDT ya es P3 (no P2 como en BTC)
        assert_eq!(motor.clasificar(50.0), 3);
        // Y 10,000 USDT ya es P9 (no P8 como en BTC)
        assert_eq!(motor.clasificar(10000.0), 9);

        println!("✓ Rangos personalizados para WLD funcionan");
    }

    #[test]
    fn test_config_invalida_orden_incorrecto() {
        let rangos = [
            40.0, 100.0, 500.0, 1000.0, 2500.0, 5000.0,
            10000.0, 50000.0, 100000.0,
            50000.0,  // ERROR: menor que el anterior
            1000000.0, 5000000.0, 10000000.0,
        ];

        let config = ConfigSegmentos::new("BTCUSDT", rangos);
        assert!(config.validar().is_err());

        println!("✓ Configuración inválida rechazada correctamente");
    }

    #[test]
    fn test_actualizar_rangos() {
        let mut motor = MotorSegmentacion::default_para("SOLUSDT");

        // Verificar rango default
        assert_eq!(motor.clasificar(50.0), 2); // P2 con rangos default

        // Actualizar rangos (SOL más pequeño)
        let nuevos = [
            20.0, 50.0, 200.0, 500.0, 1000.0, 2000.0,
            5000.0, 10000.0, 50000.0, 100000.0,
            250000.0, 500000.0, 1000000.0,
        ];
        motor.actualizar_rangos(nuevos).unwrap();

        // Ahora 50 USDT cae en P3 (no P2)
        assert_eq!(motor.clasificar(50.0), 3);

        println!("✓ Rangos actualizados sin recompilar");
    }

    #[test]
    fn test_rango_texto() {
        let config = ConfigSegmentos::default_para("BTCUSDT");

        assert_eq!(config.rango_texto(1), "0 - 40");
        assert_eq!(config.rango_texto(2), "40 - 100");
        assert_eq!(config.rango_texto(8), "10000 - 50000");
        assert_eq!(config.rango_texto(14), "10000000+");

        println!("✓ Texto de rangos correcto");
    }

    #[test]
    fn test_display_config() {
        let config = ConfigSegmentos::default_para("BTCUSDT");
        let texto = format!("{}", config);
        assert!(texto.contains("P1"));
        assert!(texto.contains("P14"));
        assert!(texto.contains("micro_retail"));
        assert!(texto.contains("market_maker"));
        println!("{}", config);
    }

    #[test]
    fn test_display_trade_segmentado() {
        let motor = MotorSegmentacion::default_para("BTCUSDT");
        let trade = trade_con_volumen(34212.50);
        let seg = motor.segmentar(trade);
        let texto = format!("{}", seg);
        assert!(texto.contains("P8"));
        assert!(texto.contains("profesional"));
        println!("{}", seg);
    }

    #[test]
    fn test_rendimiento_clasificacion() {
        let motor = MotorSegmentacion::default_para("BTCUSDT");

        // Clasificar 1 millón de trades
        let inicio = std::time::Instant::now();
        for i in 0..1_000_000u64 {
            let vol = (i % 15_000_000) as f64 + 0.01;
            let _ = motor.clasificar(vol);
        }
        let duracion = inicio.elapsed();

        println!(
            "✓ 1,000,000 clasificaciones en {:.2?} ({:.0} trades/seg)",
            duracion,
            1_000_000.0 / duracion.as_secs_f64()
        );

        // Debe ser < 100ms para 1M trades
        assert!(duracion.as_millis() < 100);
    }
}
