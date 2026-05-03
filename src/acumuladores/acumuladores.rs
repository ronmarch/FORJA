// ═══════════════════════════════════════════════════════
// FORJA v1.0 - Acumuladores v3: 9 Ventanas Deslizantes
// ═══════════════════════════════════════════════════════
// 3m, 5m, 15m, 1h, 2h, 4h, 8h, 12h, 24h
// Todas con la misma lógica: buffer circular de paquetes
// de 1 segundo. Se llenan una vez, deslizan para siempre.
// Solo se resetean con stop programado.
// ═══════════════════════════════════════════════════════

use crate::normalizador::normalizador::{SIDE_BUY, SIDE_SELL};
use crate::segmentacion::segmentacion::{TradeSegmentado, NOMBRES_CORTOS, TOTAL_SEGMENTOS};
use std::collections::VecDeque;

// ───────────────────────────────────────────────────────
// Constantes
// ───────────────────────────────────────────────────────

pub const TOTAL_TEMPORALIDADES: usize = 9;

pub const DURACIONES_SEG: [u64; TOTAL_TEMPORALIDADES] = [
    180,     // 3m
    300,     // 5m
    900,     // 15m
    3_600,   // 1h
    7_200,   // 2h
    14_400,  // 4h
    28_800,  // 8h
    43_200,  // 12h
    86_400,  // 24h
];

pub const NOMBRES_TEMPORALIDADES: [&str; TOTAL_TEMPORALIDADES] = [
    "3m", "5m", "15m", "1h", "2h", "4h", "8h", "12h", "24h",
];

// ───────────────────────────────────────────────────────
// Métricas de 1 segundo
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Default)]
pub struct MetricasPaquete {
    pub volume_total: f64,
    pub volume_buy: f64,
    pub volume_sell: f64,
    pub trade_count: u64,
}

impl MetricasPaquete {
    fn agregar(&mut self, volume_usdt: f64, side: u8) {
        self.volume_total += volume_usdt;
        self.trade_count += 1;
        match side {
            SIDE_BUY => self.volume_buy += volume_usdt,
            SIDE_SELL => self.volume_sell += volume_usdt,
            _ => {}
        }
    }
}

#[derive(Debug, Clone)]
pub struct PaqueteSegundo {
    pub timestamp_seg: u64,
    pub segmentos: [MetricasPaquete; TOTAL_SEGMENTOS],
    pub global: MetricasPaquete,
}

impl PaqueteSegundo {
    fn new(timestamp_seg: u64) -> Self {
        Self {
            timestamp_seg,
            segmentos: [MetricasPaquete::default(); TOTAL_SEGMENTOS],
            global: MetricasPaquete::default(),
        }
    }

    fn agregar_trade(&mut self, trade: &TradeSegmentado) {
        let idx = (trade.segment_id as usize).saturating_sub(1);
        if idx < TOTAL_SEGMENTOS {
            self.segmentos[idx].agregar(trade.trade.volume_usdt, trade.trade.side);
        }
        self.global.agregar(trade.trade.volume_usdt, trade.trade.side);
    }
}

// ───────────────────────────────────────────────────────
// Métricas acumuladas de ventana
// ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Default)]
pub struct MetricasVentana {
    pub volume_total: f64,
    pub volume_buy: f64,
    pub volume_sell: f64,
    pub trade_count: u64,
}

impl MetricasVentana {
    fn sumar(&mut self, p: &MetricasPaquete) {
        self.volume_total += p.volume_total;
        self.volume_buy += p.volume_buy;
        self.volume_sell += p.volume_sell;
        self.trade_count += p.trade_count;
    }

    fn restar(&mut self, p: &MetricasPaquete) {
        self.volume_total -= p.volume_total;
        self.volume_buy -= p.volume_buy;
        self.volume_sell -= p.volume_sell;
        self.trade_count = self.trade_count.saturating_sub(p.trade_count);
        if self.volume_total < 0.0 { self.volume_total = 0.0; }
        if self.volume_buy < 0.0 { self.volume_buy = 0.0; }
        if self.volume_sell < 0.0 { self.volume_sell = 0.0; }
    }

    pub fn pct_buy(&self) -> f64 {
        if self.volume_total > 0.0 { (self.volume_buy / self.volume_total) * 100.0 }
        else { 0.0 }
    }

    pub fn pct_sell(&self) -> f64 {
        if self.volume_total > 0.0 { (self.volume_sell / self.volume_total) * 100.0 }
        else { 0.0 }
    }

    pub fn activo(&self) -> bool {
        self.trade_count > 0
    }
}

// ───────────────────────────────────────────────────────
// Temporalidad (ventana deslizante)
// ───────────────────────────────────────────────────────

pub struct Temporalidad {
    idx: usize,
    capacidad: usize,
    paquetes: VecDeque<PaqueteSegundo>,
    segmentos: [MetricasVentana; TOTAL_SEGMENTOS],
    global: MetricasVentana,
}

impl Temporalidad {
    fn new(idx: usize) -> Self {
        Self {
            idx,
            capacidad: DURACIONES_SEG[idx] as usize,
            paquetes: VecDeque::with_capacity(DURACIONES_SEG[idx] as usize + 1),
            segmentos: [MetricasVentana::default(); TOTAL_SEGMENTOS],
            global: MetricasVentana::default(),
        }
    }

    pub fn agregar_paquete(&mut self, paquete: &PaqueteSegundo) {
        if self.paquetes.len() >= self.capacidad {
            if let Some(viejo) = self.paquetes.pop_front() {
                for i in 0..TOTAL_SEGMENTOS {
                    self.segmentos[i].restar(&viejo.segmentos[i]);
                }
                self.global.restar(&viejo.global);
            }
        }
        for i in 0..TOTAL_SEGMENTOS {
            self.segmentos[i].sumar(&paquete.segmentos[i]);
        }
        self.global.sumar(&paquete.global);
        self.paquetes.push_back(paquete.clone());
    }

    pub fn nombre(&self) -> &'static str { NOMBRES_TEMPORALIDADES[self.idx] }
    pub fn segmento(&self, segment_id: u8) -> &MetricasVentana {
        let idx = (segment_id as usize).saturating_sub(1);
        if idx < TOTAL_SEGMENTOS { &self.segmentos[idx] } else { &self.segmentos[0] }
    }
    pub fn global(&self) -> &MetricasVentana { &self.global }
    pub fn llenado_pct(&self) -> f64 { (self.paquetes.len() as f64 / self.capacidad as f64) * 100.0 }

    fn resetear(&mut self) {
        self.paquetes.clear();
        self.segmentos = [MetricasVentana::default(); TOTAL_SEGMENTOS];
        self.global = MetricasVentana::default();
    }
}

// ───────────────────────────────────────────────────────
// Colector de ticks por segundo
// ───────────────────────────────────────────────────────

pub struct ColectorSegundo {
    paquete_actual: PaqueteSegundo,
    segundo_actual: u64,
}

impl ColectorSegundo {
    pub fn new() -> Self {
        Self {
            paquete_actual: PaqueteSegundo::new(0),
            segundo_actual: 0,
        }
    }

    pub fn agregar(&mut self, trade: &TradeSegmentado) -> Option<PaqueteSegundo> {
        let ts_seg = trade.trade.timestamp / 1000;
        if self.segundo_actual == 0 {
            self.segundo_actual = ts_seg;
            self.paquete_actual = PaqueteSegundo::new(ts_seg);
            self.paquete_actual.agregar_trade(trade);
            return None;
        }
        if ts_seg != self.segundo_actual {
            let completado = self.paquete_actual.clone();
            self.segundo_actual = ts_seg;
            self.paquete_actual = PaqueteSegundo::new(ts_seg);
            self.paquete_actual.agregar_trade(trade);
            return Some(completado);
        }
        self.paquete_actual.agregar_trade(trade);
        None
    }

    pub fn flush(&mut self) -> Option<PaqueteSegundo> {
        if self.paquete_actual.global.trade_count > 0 {
            let paquete = self.paquete_actual.clone();
            self.paquete_actual = PaqueteSegundo::new(self.segundo_actual);
            Some(paquete)
        } else { None }
    }
}

// ───────────────────────────────────────────────────────
// Gestor de Acumuladores
// ───────────────────────────────────────────────────────

pub struct GestorAcumuladores {
    nombre: String,
    colector: ColectorSegundo,
    temporalidades: Vec<Temporalidad>,
    trades_procesados: u64,
    paquetes_emitidos: u64,
}

impl GestorAcumuladores {
    pub fn new(nombre: &str) -> Self {
        let temporalidades = (0..TOTAL_TEMPORALIDADES).map(|i| Temporalidad::new(i)).collect();
        tracing::info!(
            "ACUMULADORES | {} | 9 temporalidades [3m,5m,15m,1h,2h,4h,8h,12h,24h]",
            nombre,
        );
        Self {
            nombre: nombre.to_string(),
            colector: ColectorSegundo::new(),
            temporalidades,
            trades_procesados: 0,
            paquetes_emitidos: 0,
        }
    }

    pub fn procesar(&mut self, trade: &TradeSegmentado) {
        self.trades_procesados += 1;
        if let Some(paquete) = self.colector.agregar(trade) {
            self.distribuir_paquete(&paquete);
        }
    }

    fn distribuir_paquete(&mut self, paquete: &PaqueteSegundo) {
        self.paquetes_emitidos += 1;
        for temp in &mut self.temporalidades {
            temp.agregar_paquete(paquete);
        }
    }

    pub fn flush(&mut self) {
        if let Some(paquete) = self.colector.flush() {
            self.distribuir_paquete(&paquete);
        }
    }

    pub fn temporalidad(&self, idx: usize) -> &Temporalidad {
        &self.temporalidades[idx.min(TOTAL_TEMPORALIDADES - 1)]
    }

    pub fn trades_procesados(&self) -> u64 { self.trades_procesados }
    pub fn nombre(&self) -> &str { &self.nombre }

    /// Solo se llama desde stop programado
    pub fn resetear(&mut self) {
        self.trades_procesados = 0;
        self.paquetes_emitidos = 0;
        self.colector = ColectorSegundo::new();
        for temp in &mut self.temporalidades {
            temp.resetear();
        }
        tracing::info!("ACUMULADORES | {} | Reseteado (stop programado)", self.nombre);
    }

    pub fn tabla_segmentos(&self, idx_temp: usize) -> String {
        let temp = &self.temporalidades[idx_temp.min(TOTAL_TEMPORALIDADES - 1)];
        let g = temp.global();
        let mut r = String::new();

        r.push_str(&format!(
            "\n  ┌──────────────────────────────────────────────────────────────────┐\n"
        ));
        r.push_str(&format!(
            "  │ {} │ {} │ Trades: {:>8} │ Vol: {:>14.2} │{:>4.0}%│\n",
            self.nombre, temp.nombre(), g.trade_count, g.volume_total, temp.llenado_pct(),
        ));
        r.push_str(
            "  ├──────┬──────────┬──────────────────┬──────────┬─────────────────┤\n",
        );
        r.push_str(
            "  │ Seg  │  Trades  │  Volumen USDT    │  %Vol    │  Buy%    Sell%  │\n",
        );
        r.push_str(
            "  ├──────┼──────────┼──────────────────┼──────────┼─────────────────┤\n",
        );

        for i in 0..TOTAL_SEGMENTOS {
            let seg = &temp.segmentos[i];
            if seg.activo() {
                let pct_vol = if g.volume_total > 0.0 {
                    (seg.volume_total / g.volume_total) * 100.0
                } else { 0.0 };
                r.push_str(&format!(
                    "  │ {:<4} │ {:>8} │ {:>16.2} │ {:>6.1}%  │ {:>5.1}%  {:>5.1}% │\n",
                    NOMBRES_CORTOS[i], seg.trade_count, seg.volume_total,
                    pct_vol, seg.pct_buy(), seg.pct_sell(),
                ));
            }
        }

        r.push_str(
            "  ├──────┼──────────┼──────────────────┼──────────┼─────────────────┤\n",
        );
        r.push_str(&format!(
            "  │ GLOB │ {:>8} │ {:>16.2} │  100.0%  │ {:>5.1}%  {:>5.1}% │\n",
            g.trade_count, g.volume_total, g.pct_buy(), g.pct_sell(),
        ));
        r.push_str(
            "  └──────┴──────────┴──────────────────┴──────────┴─────────────────┘\n",
        );
        r
    }

    pub fn resumen_temporalidades(&self) -> String {
        let mut r = String::new();
        r.push_str(&format!(
            "\n  ┌─────────────────────────────────────────────────────────────┐\n"
        ));
        r.push_str(&format!(
            "  │ {} │ Trades: {:>10} │ Paq: {:>8}         │\n",
            self.nombre, self.trades_procesados, self.paquetes_emitidos,
        ));
        r.push_str(
            "  ├──────┬────────┬──────────┬──────────────────┬──────────────┤\n",
        );
        r.push_str(
            "  │  TF  │  Fill  │  Trades  │  Volumen USDT    │ Buy%  Sell%  │\n",
        );
        r.push_str(
            "  ├──────┼────────┼──────────┼──────────────────┼──────────────┤\n",
        );
        for temp in &self.temporalidades {
            let g = temp.global();
            r.push_str(&format!(
                "  │ {:>4} │ {:>4.0}%  │ {:>8} │ {:>16.2} │{:>5.1}% {:>5.1}%│\n",
                temp.nombre(), temp.llenado_pct(), g.trade_count,
                g.volume_total, g.pct_buy(), g.pct_sell(),
            ));
        }
        r.push_str(
            "  └──────┴────────┴──────────┴──────────────────┴──────────────┘\n",
        );
        r
    }
}

impl std::fmt::Display for GestorAcumuladores {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.resumen_temporalidades())
    }
}

// ───────────────────────────────────────────────────────
// Tests
// ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::normalizador::normalizador::TradeFORJA;

    fn trade_seg(ts: u64, vol: f64, side: u8, seg: u8) -> TradeSegmentado {
        TradeSegmentado {
            trade: TradeFORJA::new(ts, 68000.0, vol, vol / 68000.0, side),
            segment_id: seg,
        }
    }

    #[test]
    fn test_9_temporalidades() {
        assert_eq!(TOTAL_TEMPORALIDADES, 9);
        assert_eq!(DURACIONES_SEG[0], 180);   // 3m
        assert_eq!(DURACIONES_SEG[1], 300);   // 5m
        assert_eq!(DURACIONES_SEG[2], 900);   // 15m
        assert_eq!(DURACIONES_SEG[3], 3_600); // 1h
        assert_eq!(DURACIONES_SEG[4], 7_200); // 2h
        assert_eq!(DURACIONES_SEG[5], 14_400);// 4h
        assert_eq!(DURACIONES_SEG[6], 28_800);// 8h
        assert_eq!(DURACIONES_SEG[7], 43_200);// 12h
        assert_eq!(DURACIONES_SEG[8], 86_400);// 24h
        println!("✓ 9 temporalidades correctas");
    }

    #[test]
    fn test_ventana_deslizante() {
        let mut temp = Temporalidad::new(0); // 3m = 180s

        for i in 0..180 {
            let mut paq = PaqueteSegundo::new(i);
            paq.agregar_trade(&trade_seg(i * 1000, 100.0, SIDE_BUY, 1));
            temp.agregar_paquete(&paq);
        }

        assert_eq!(temp.paquetes.len(), 180);
        assert!((temp.global().volume_total - 18000.0).abs() < 0.01);
        assert!((temp.llenado_pct() - 100.0).abs() < 0.1);

        // Agregar uno más: desliza
        let mut paq = PaqueteSegundo::new(180);
        paq.agregar_trade(&trade_seg(180_000, 500.0, SIDE_SELL, 1));
        temp.agregar_paquete(&paq);

        assert_eq!(temp.paquetes.len(), 180); // Sigue en 180
        assert!((temp.global().volume_total - 18400.0).abs() < 0.01);
        assert!((temp.global().volume_buy - 17900.0).abs() < 0.01);
        assert!((temp.global().volume_sell - 500.0).abs() < 0.01);
        println!("✓ Deslizamiento correcto");
    }

    #[test]
    fn test_temporalidades_diferentes_datos() {
        let mut gestor = GestorAcumuladores::new("TEST");

        // 200 segundos: primeros 100 buy, últimos 100 sell
        for seg in 0..200u64 {
            let side = if seg < 100 { SIDE_BUY } else { SIDE_SELL };
            for tick in 0..5 {
                gestor.procesar(&trade_seg(seg * 1000 + tick * 100, 100.0, side, 3));
            }
        }
        gestor.flush();

        let t3m = gestor.temporalidad(0);  // 180s → ve seg 20-199
        let t5m = gestor.temporalidad(1);  // 300s → ve todo

        let buy_3m = t3m.global().pct_buy();
        let buy_5m = t5m.global().pct_buy();

        // 3m perdió 20 seg de buys → menos buy% que 5m
        assert!(buy_3m < buy_5m);
        println!("✓ 3m Buy%: {:.1}% vs 5m Buy%: {:.1}%", buy_3m, buy_5m);
    }

    #[test]
    fn test_porcentajes_por_segmento() {
        let mut temp = Temporalidad::new(0);
        let mut paq = PaqueteSegundo::new(0);

        paq.agregar_trade(&trade_seg(0, 700.0, SIDE_BUY, 1));
        paq.agregar_trade(&trade_seg(100, 300.0, SIDE_SELL, 1));
        paq.agregar_trade(&trade_seg(200, 200.0, SIDE_BUY, 8));
        paq.agregar_trade(&trade_seg(300, 800.0, SIDE_SELL, 8));
        temp.agregar_paquete(&paq);

        assert!((temp.segmento(1).pct_buy() - 70.0).abs() < 0.1);
        assert!((temp.segmento(8).pct_buy() - 20.0).abs() < 0.1);
        assert!((temp.global().pct_buy() - 45.0).abs() < 0.1);
        println!("✓ B%/S% correctos por segmento");
    }

    #[test]
    fn test_sin_reseteo_automatico() {
        let mut temp = Temporalidad::new(0); // 3m

        // Llenar
        for i in 0..180 {
            let mut paq = PaqueteSegundo::new(i);
            paq.agregar_trade(&trade_seg(i * 1000, 100.0, SIDE_BUY, 1));
            temp.agregar_paquete(&paq);
        }

        // Seguir 1000 segundos más: nunca se resetea
        for i in 180..1180 {
            let mut paq = PaqueteSegundo::new(i);
            paq.agregar_trade(&trade_seg(i * 1000, 100.0, SIDE_SELL, 1));
            temp.agregar_paquete(&paq);
        }

        // Siempre tiene 180 paquetes, nunca se vació
        assert_eq!(temp.paquetes.len(), 180);
        assert!((temp.global().volume_total - 18000.0).abs() < 0.01);
        // Los últimos 180 son todos sell
        assert!((temp.global().pct_sell() - 100.0).abs() < 0.1);
        println!("✓ Sin reseteo automático, siempre deslizando");
    }

    #[test]
    fn test_rendimiento() {
        let mut gestor = GestorAcumuladores::new("BENCH");
        let inicio = std::time::Instant::now();
        let mut ts = 1_000_000u64;

        for i in 0..1_000_000u64 {
            let seg = ((i % 14) + 1) as u8;
            let side = if i % 2 == 0 { SIDE_BUY } else { SIDE_SELL };
            gestor.procesar(&trade_seg(ts, 100.0, side, seg));
            ts += 1;
        }

        let duracion = inicio.elapsed();
        println!(
            "✓ 1M trades en 9 temporalidades: {:.2?} ({:.0} t/s)",
            duracion, 1_000_000.0 / duracion.as_secs_f64()
        );
        assert!(duracion.as_millis() < 1500);
    }
}
