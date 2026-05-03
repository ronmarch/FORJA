// ═══════════════════════════════════════════════════════
// FORJA v1.0 - Buffer Circular de 24 horas
// ═══════════════════════════════════════════════════════
// Buffer que almacena trades normalizados en memoria
// con ventana deslizante de 24 horas.
//
// Características:
//   - VecDeque como estructura base (eficiente para
//     push_back y pop_front)
//   - Purga por timestamp: descarta trades con más
//     de 24 horas de antigüedad
//   - Purga lazy: se ejecuta cada N inserciones para
//     no penalizar cada trade individual
//   - Thread-safe vía Arc<RwLock> en el gestor
//   - Estadísticas en tiempo real sin recorrer el buffer
// ═══════════════════════════════════════════════════════

use crate::normalizador::normalizador::{SIDE_BUY, SIDE_SELL};
use crate::parquet_writer::escritor::TradeConSeg;
use std::collections::VecDeque;

// ───────────────────────────────────────────────────────
// Constantes
// ───────────────────────────────────────────────────────

/// Ventana de retención en milisegundos (24 horas)
const VENTANA_24H_MS: u64 = 24 * 60 * 60 * 1000;

/// Ejecutar purga cada N inserciones (evita purgar en cada trade)
const PURGA_CADA_N: u64 = 500;

/// Capacidad inicial del buffer (pre-alocación)
/// SOL/altcoins: ~200K spot + ~600K futures/día
/// BTC/ETH: ~500K spot + ~1.5M futures/día
/// 500K es suficiente para 24h de SOL y cubre ETH/BTC con deslizamiento
const CAPACIDAD_INICIAL: usize = 500_000;

// ───────────────────────────────────────────────────────
// Estadísticas en tiempo real
// ───────────────────────────────────────────────────────

/// Estadísticas mantenidas incrementalmente.
/// Se actualizan con cada inserción/purga sin necesidad
/// de recorrer el buffer completo.
#[derive(Debug, Clone, Copy)]
pub struct BufferStats {
    /// Cantidad total de trades en el buffer
    pub total_trades: u64,
    /// Volumen total en USDT
    pub volumen_total_usdt: f64,
    /// Volumen Buy en USDT
    pub volumen_buy_usdt: f64,
    /// Volumen Sell en USDT
    pub volumen_sell_usdt: f64,
    /// Cantidad de trades Buy
    pub trades_buy: u64,
    /// Cantidad de trades Sell
    pub trades_sell: u64,
    /// Timestamp del trade más antiguo en el buffer
    pub timestamp_inicio: u64,
    /// Timestamp del trade más reciente
    pub timestamp_fin: u64,
    /// Total de trades insertados desde el inicio (incluyendo purgados)
    pub trades_insertados_total: u64,
    /// Total de trades purgados
    pub trades_purgados_total: u64,
}

impl BufferStats {
    fn new() -> Self {
        Self {
            total_trades: 0,
            volumen_total_usdt: 0.0,
            volumen_buy_usdt: 0.0,
            volumen_sell_usdt: 0.0,
            trades_buy: 0,
            trades_sell: 0,
            timestamp_inicio: 0,
            timestamp_fin: 0,
            trades_insertados_total: 0,
            trades_purgados_total: 0,
        }
    }

    /// Agrega un trade a las estadísticas
    fn agregar(&mut self, trade: &TradeConSeg) {
        self.total_trades += 1;
        self.volumen_total_usdt += trade.trade.volume_usdt;
        self.trades_insertados_total += 1;

        match trade.trade.side {
            SIDE_BUY => {
                self.volumen_buy_usdt += trade.trade.volume_usdt;
                self.trades_buy += 1;
            }
            SIDE_SELL => {
                self.volumen_sell_usdt += trade.trade.volume_usdt;
                self.trades_sell += 1;
            }
            _ => {}
        }

        // Actualizar timestamps
        if self.timestamp_inicio == 0 || trade.trade.timestamp < self.timestamp_inicio {
            self.timestamp_inicio = trade.trade.timestamp;
        }
        if trade.trade.timestamp > self.timestamp_fin {
            self.timestamp_fin = trade.trade.timestamp;
        }
    }

    /// Resta un trade de las estadísticas (al purgar)
    fn restar(&mut self, trade: &TradeConSeg) {
        self.total_trades = self.total_trades.saturating_sub(1);
        self.volumen_total_usdt -= trade.trade.volume_usdt;
        self.trades_purgados_total += 1;

        match trade.trade.side {
            SIDE_BUY => {
                self.volumen_buy_usdt -= trade.trade.volume_usdt;
                self.trades_buy = self.trades_buy.saturating_sub(1);
            }
            SIDE_SELL => {
                self.volumen_sell_usdt -= trade.trade.volume_usdt;
                self.trades_sell = self.trades_sell.saturating_sub(1);
            }
            _ => {}
        }

        // Corregir posibles errores de punto flotante
        if self.volumen_total_usdt < 0.0 {
            self.volumen_total_usdt = 0.0;
        }
        if self.volumen_buy_usdt < 0.0 {
            self.volumen_buy_usdt = 0.0;
        }
        if self.volumen_sell_usdt < 0.0 {
            self.volumen_sell_usdt = 0.0;
        }
    }

    /// Delta actual (buy - sell)
    pub fn delta(&self) -> f64 {
        self.volumen_buy_usdt - self.volumen_sell_usdt
    }

    /// Porcentaje de volumen Buy
    pub fn pct_buy(&self) -> f64 {
        if self.volumen_total_usdt > 0.0 {
            (self.volumen_buy_usdt / self.volumen_total_usdt) * 100.0
        } else {
            0.0
        }
    }

    /// Porcentaje de volumen Sell
    pub fn pct_sell(&self) -> f64 {
        if self.volumen_total_usdt > 0.0 {
            (self.volumen_sell_usdt / self.volumen_total_usdt) * 100.0
        } else {
            0.0
        }
    }
}

impl std::fmt::Display for BufferStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Trades: {} | Vol: {:.2} USDT | Buy: {:.1}% | Sell: {:.1}% | Delta: {:.2}",
            self.total_trades,
            self.volumen_total_usdt,
            self.pct_buy(),
            self.pct_sell(),
            self.delta(),
        )
    }
}

// ───────────────────────────────────────────────────────
// Buffer de trades (single-thread, sin lock)
// ───────────────────────────────────────────────────────

/// Buffer circular de trades con ventana de 24 horas.
/// Esta estructura NO es thread-safe por sí sola.
/// El acceso concurrente se gestiona en BufferManager.
pub struct TradeBuffer {
    /// Nombre identificador (ej: "BTCUSDT_SPOT")
    nombre: String,
    /// Cola de trades ordenados por timestamp
    trades: VecDeque<TradeConSeg>,
    /// Estadísticas incrementales
    stats: BufferStats,
    /// Contador de inserciones para trigger de purga
    contador_inserciones: u64,
}

impl TradeBuffer {
    /// Crea un nuevo buffer con capacidad pre-alocada
    pub fn new(nombre: &str) -> Self {
        Self {
            nombre: nombre.to_string(),
            trades: VecDeque::with_capacity(CAPACIDAD_INICIAL),
            stats: BufferStats::new(),
            contador_inserciones: 0,
        }
    }

    /// Crea un buffer con capacidad personalizada
    pub fn con_capacidad(nombre: &str, capacidad: usize) -> Self {
        Self {
            nombre: nombre.to_string(),
            trades: VecDeque::with_capacity(capacidad),
            stats: BufferStats::new(),
            contador_inserciones: 0,
        }
    }

    /// Inserta un trade en el buffer.
    /// Ejecuta purga lazy cada PURGA_CADA_N inserciones.
    pub fn insertar(&mut self, trade: TradeConSeg) {
        // Actualizar estadísticas
        self.stats.agregar(&trade);

        // Insertar al final (los trades llegan en orden cronológico)
        self.trades.push_back(trade);

        // Purga lazy
        self.contador_inserciones += 1;
        if self.contador_inserciones % PURGA_CADA_N == 0 {
            self.purgar();
        }
    }

    /// Purga trades fuera de la ventana de 24 horas.
    /// Elimina desde el frente (los más antiguos) hasta
    /// encontrar uno dentro de la ventana.
    pub fn purgar(&mut self) {
        if self.trades.is_empty() {
            return;
        }

        // Timestamp de corte: ahora - 24 horas
        // Usamos el trade más reciente como referencia de "ahora"
        let ahora = self.stats.timestamp_fin;
        if ahora == 0 {
            return;
        }
        let corte = ahora.saturating_sub(VENTANA_24H_MS);

        // Eliminar trades anteriores al corte
        let mut purgados = 0u64;
        while let Some(front) = self.trades.front() {
            if front.trade.timestamp < corte {
                if let Some(trade_viejo) = self.trades.pop_front() {
                    self.stats.restar(&trade_viejo);
                    purgados += 1;
                }
            } else {
                break; // Los trades están ordenados, podemos parar
            }
        }

        // Actualizar timestamp de inicio
        if let Some(front) = self.trades.front() {
            self.stats.timestamp_inicio = front.trade.timestamp;
        }

        if purgados > 0 {
            tracing::debug!(
                "BUFFER {} | Purgados: {} trades | Quedan: {}",
                self.nombre,
                purgados,
                self.trades.len()
            );
        }
    }

    /// Fuerza una purga completa (usado antes de volcado a Parquet)
    pub fn purgar_forzado(&mut self) {
        self.purgar();
    }

    /// Retorna las estadísticas actuales (sin recorrer el buffer)
    pub fn stats(&self) -> &BufferStats {
        &self.stats
    }

    /// Retorna la cantidad de trades en el buffer
    pub fn len(&self) -> usize {
        self.trades.len()
    }

    /// Retorna si el buffer está vacío
    pub fn esta_vacio(&self) -> bool {
        self.trades.is_empty()
    }

    /// Retorna el nombre del buffer
    pub fn nombre(&self) -> &str {
        &self.nombre
    }

    /// Retorna estimación de memoria usada en bytes
    pub fn memoria_bytes(&self) -> usize {
        self.trades.len() * std::mem::size_of::<TradeConSeg>()
    }

    /// Retorna estimación de memoria usada en MB
    pub fn memoria_mb(&self) -> f64 {
        self.memoria_bytes() as f64 / (1024.0 * 1024.0)
    }

    /// Obtiene trades en un rango de tiempo (para acumuladores)
    /// Retorna un slice de trades entre timestamp_inicio y timestamp_fin
    pub fn trades_en_rango(&self, ts_inicio: u64, ts_fin: u64) -> Vec<&TradeConSeg> {
        self.trades
            .iter()
            .filter(|t| t.trade.timestamp >= ts_inicio && t.trade.timestamp <= ts_fin)
            .collect()
    }

    /// Obtiene los últimos N trades
    pub fn ultimos_n(&self, n: usize) -> Vec<&TradeConSeg> {
        let len = self.trades.len();
        if n >= len {
            self.trades.iter().collect()
        } else {
            self.trades.iter().skip(len - n).collect()
        }
    }

    /// Obtiene todos los trades (para volcado a Parquet)
    pub fn todos_los_trades(&self) -> &VecDeque<TradeConSeg> {
        &self.trades
    }

    /// Limpia completamente el buffer (usado en stop programado
    /// después del volcado a Parquet)
    pub fn limpiar(&mut self) {
        self.trades.clear();
        self.stats = BufferStats::new();
        self.contador_inserciones = 0;
        tracing::info!("BUFFER {} | Limpiado completamente", self.nombre);
    }
}

impl std::fmt::Display for TradeBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BUFFER {} | {} trades | {:.2} MB | {}",
            self.nombre,
            self.trades.len(),
            self.memoria_mb(),
            self.stats,
        )
    }
}

// ───────────────────────────────────────────────────────
// Gestor de Buffers (thread-safe)
// ───────────────────────────────────────────────────────

use crate::normalizador::normalizador::Mercado;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Identificador único de un buffer
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct BufferId {
    pub simbolo: String,
    pub mercado: Mercado,
}

impl BufferId {
    pub fn new(simbolo: &str, mercado: Mercado) -> Self {
        Self {
            simbolo: simbolo.to_uppercase(),
            mercado,
        }
    }

    /// Nombre legible para el buffer
    pub fn nombre(&self) -> String {
        format!("{}_{}", self.simbolo, self.mercado)
    }
}

impl std::fmt::Display for BufferId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{}", self.simbolo, self.mercado)
    }
}

/// Gestor de múltiples buffers con acceso thread-safe.
/// Cada buffer está protegido individualmente con RwLock,
/// permitiendo escritura simultánea en buffers diferentes.
pub struct BufferManager {
    buffers: Vec<(BufferId, Arc<RwLock<TradeBuffer>>)>,
}

impl BufferManager {
    /// Crea un nuevo gestor sin buffers
    pub fn new() -> Self {
        Self {
            buffers: Vec::new(),
        }
    }

    /// Registra un nuevo buffer para un par y mercado
    pub fn registrar(&mut self, simbolo: &str, mercado: Mercado) {
        let id = BufferId::new(simbolo, mercado);
        let nombre = id.nombre();

        // Verificar que no exista ya
        if self.buscar_buffer(&id).is_some() {
            tracing::warn!("BUFFER_MGR | Buffer {} ya existe, ignorando", nombre);
            return;
        }

        let buffer = Arc::new(RwLock::new(TradeBuffer::new(&nombre)));
        self.buffers.push((id.clone(), buffer));
        tracing::info!("BUFFER_MGR | Registrado: {}", nombre);
    }

    /// Registra un buffer con capacidad personalizada
    pub fn registrar_con_capacidad(
        &mut self,
        simbolo: &str,
        mercado: Mercado,
        capacidad: usize,
    ) {
        let id = BufferId::new(simbolo, mercado);
        let nombre = id.nombre();

        if self.buscar_buffer(&id).is_some() {
            tracing::warn!("BUFFER_MGR | Buffer {} ya existe, ignorando", nombre);
            return;
        }

        let buffer = Arc::new(RwLock::new(TradeBuffer::con_capacidad(&nombre, capacidad)));
        self.buffers.push((id.clone(), buffer));
        tracing::info!(
            "BUFFER_MGR | Registrado: {} (capacidad: {})",
            nombre,
            capacidad
        );
    }

    /// Obtiene referencia al buffer por ID
    pub fn buscar_buffer(&self, id: &BufferId) -> Option<Arc<RwLock<TradeBuffer>>> {
        self.buffers
            .iter()
            .find(|(bid, _)| bid == id)
            .map(|(_, buf)| Arc::clone(buf))
    }

    /// Obtiene referencia al buffer por símbolo y mercado
    pub fn obtener(&self, simbolo: &str, mercado: Mercado) -> Option<Arc<RwLock<TradeBuffer>>> {
        let id = BufferId::new(simbolo, mercado);
        self.buscar_buffer(&id)
    }

    /// Inserta un trade en el buffer correspondiente
    pub async fn insertar_trade(
        &self,
        simbolo: &str,
        mercado: Mercado,
        trade: TradeConSeg,
    ) -> bool {
        let id = BufferId::new(simbolo, mercado);
        if let Some(buffer) = self.buscar_buffer(&id) {
            let mut buf = buffer.write().await;
            buf.insertar(trade);
            true
        } else {
            tracing::error!(
                "BUFFER_MGR | Buffer no encontrado: {}_{}",
                simbolo,
                mercado
            );
            false
        }
    }

    /// Retorna estadísticas de todos los buffers
    pub async fn stats_todos(&self) -> Vec<(BufferId, BufferStats)> {
        let mut resultado = Vec::with_capacity(self.buffers.len());
        for (id, buffer) in &self.buffers {
            let buf = buffer.read().await;
            resultado.push((id.clone(), *buf.stats()));
        }
        resultado
    }

    /// Retorna memoria total usada por todos los buffers en MB
    pub async fn memoria_total_mb(&self) -> f64 {
        let mut total = 0.0;
        for (_, buffer) in &self.buffers {
            let buf = buffer.read().await;
            total += buf.memoria_mb();
        }
        total
    }

    /// Limpia todos los buffers (stop programado)
    pub async fn limpiar_todos(&self) {
        for (_, buffer) in &self.buffers {
            let mut buf = buffer.write().await;
            buf.limpiar();
        }
        tracing::info!("BUFFER_MGR | Todos los buffers limpiados");
    }

    /// Cantidad de buffers registrados
    pub fn cantidad(&self) -> usize {
        self.buffers.len()
    }

    /// Retorna los IDs de todos los buffers
    pub fn ids(&self) -> Vec<&BufferId> {
        self.buffers.iter().map(|(id, _)| id).collect()
    }
}

impl std::fmt::Display for BufferManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BUFFER_MGR | {} buffers registrados", self.buffers.len())
    }
}

// ───────────────────────────────────────────────────────
// Tests
// ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn crear_trade(timestamp: u64, volume_usdt: f64, side: u8) -> TradeFORJA {
        TradeFORJA::new(
            timestamp,
            68000.0,      // precio fijo para tests
            volume_usdt,
            volume_usdt / 68000.0, // volume_base derivado
            side,
        )
    }

    fn ahora_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    #[test]
    fn test_buffer_insertar_y_stats() {
        let mut buf = TradeBuffer::new("TEST_SPOT");

        let ts = ahora_ms();
        buf.insertar(crear_trade(ts, 100.0, SIDE_BUY));
        buf.insertar(crear_trade(ts + 1, 200.0, SIDE_SELL));
        buf.insertar(crear_trade(ts + 2, 300.0, SIDE_BUY));

        assert_eq!(buf.len(), 3);
        assert_eq!(buf.stats().total_trades, 3);
        assert!((buf.stats().volumen_total_usdt - 600.0).abs() < 0.01);
        assert!((buf.stats().volumen_buy_usdt - 400.0).abs() < 0.01);
        assert!((buf.stats().volumen_sell_usdt - 200.0).abs() < 0.01);
        assert_eq!(buf.stats().trades_buy, 2);
        assert_eq!(buf.stats().trades_sell, 1);

        println!("{}", buf);
    }

    #[test]
    fn test_buffer_delta_y_porcentajes() {
        let mut buf = TradeBuffer::new("TEST_FUTURES");

        let ts = ahora_ms();
        buf.insertar(crear_trade(ts, 600.0, SIDE_BUY));
        buf.insertar(crear_trade(ts + 1, 400.0, SIDE_SELL));

        // Delta = 600 - 400 = 200
        assert!((buf.stats().delta() - 200.0).abs() < 0.01);
        // Buy% = 60%
        assert!((buf.stats().pct_buy() - 60.0).abs() < 0.01);
        // Sell% = 40%
        assert!((buf.stats().pct_sell() - 40.0).abs() < 0.01);
    }

    #[test]
    fn test_buffer_purga_por_tiempo() {
        let mut buf = TradeBuffer::new("TEST_PURGA");

        let ts = ahora_ms();

        // Insertar trade de "hace 25 horas" (fuera de ventana)
        let ts_viejo = ts - VENTANA_24H_MS - 3_600_000; // 25 horas atrás
        buf.insertar(crear_trade(ts_viejo, 1000.0, SIDE_BUY));

        // Insertar trade actual
        buf.insertar(crear_trade(ts, 500.0, SIDE_SELL));

        assert_eq!(buf.len(), 2);

        // Forzar purga
        buf.purgar();

        // El trade viejo debería haberse eliminado
        assert_eq!(buf.len(), 1);
        assert!((buf.stats().volumen_total_usdt - 500.0).abs() < 0.01);
        assert_eq!(buf.stats().trades_purgados_total, 1);
    }

    #[test]
    fn test_buffer_ultimos_n() {
        let mut buf = TradeBuffer::new("TEST_ULTIMOS");

        let ts = ahora_ms();
        for i in 0..10 {
            buf.insertar(crear_trade(ts + i, (i as f64 + 1.0) * 100.0, SIDE_BUY));
        }

        let ultimos_3 = buf.ultimos_n(3);
        assert_eq!(ultimos_3.len(), 3);
        // El último trade debería tener volume_usdt = 1000.0
        assert!((ultimos_3[2].volume_usdt - 1000.0).abs() < 0.01);
    }

    #[test]
    fn test_buffer_trades_en_rango() {
        let mut buf = TradeBuffer::new("TEST_RANGO");

        let ts = ahora_ms();
        for i in 0..100 {
            buf.insertar(crear_trade(ts + i * 1000, 50.0, SIDE_BUY)); // 1 trade por segundo
        }

        // Buscar trades en un rango de 10 segundos
        let rango = buf.trades_en_rango(ts + 20_000, ts + 30_000);
        assert_eq!(rango.len(), 11); // 20, 21, ..., 30 = 11 trades
    }

    #[test]
    fn test_buffer_limpiar() {
        let mut buf = TradeBuffer::new("TEST_LIMPIAR");

        let ts = ahora_ms();
        for i in 0..100 {
            buf.insertar(crear_trade(ts + i, 100.0, SIDE_BUY));
        }

        assert_eq!(buf.len(), 100);
        buf.limpiar();
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.stats().total_trades, 0);
        assert!((buf.stats().volumen_total_usdt).abs() < 0.01);
    }

    #[test]
    fn test_buffer_memoria() {
        let mut buf = TradeBuffer::new("TEST_MEM");

        let ts = ahora_ms();
        for i in 0..1000 {
            buf.insertar(crear_trade(ts + i, 100.0, SIDE_BUY));
        }

        // TradeFORJA = 33 bytes teóricos, pero Rust alinea a 40 bytes (8+8+8+8+1 + padding)
        let mem = buf.memoria_bytes();
        assert!(mem > 0);
        println!(
            "1000 trades = {} bytes ({:.2} KB) | sizeof(TradeFORJA) = {}",
            mem,
            mem as f64 / 1024.0,
            std::mem::size_of::<TradeFORJA>()
        );
    }

    #[test]
    fn test_buffer_display() {
        let mut buf = TradeBuffer::new("BTCUSDT_SPOT");

        let ts = ahora_ms();
        buf.insertar(crear_trade(ts, 50000.0, SIDE_BUY));
        buf.insertar(crear_trade(ts + 1, 30000.0, SIDE_SELL));

        let display = format!("{}", buf);
        assert!(display.contains("BTCUSDT_SPOT"));
        assert!(display.contains("2 trades"));
        println!("{}", display);
    }

    // ── Tests del BufferManager ──

    #[tokio::test]
    async fn test_manager_registrar_y_obtener() {
        let mut mgr = BufferManager::new();
        mgr.registrar("BTCUSDT", Mercado::Spot);
        mgr.registrar("BTCUSDT", Mercado::Futures);

        assert_eq!(mgr.cantidad(), 2);

        let buf_spot = mgr.obtener("BTCUSDT", Mercado::Spot);
        assert!(buf_spot.is_some());

        let buf_futures = mgr.obtener("BTCUSDT", Mercado::Futures);
        assert!(buf_futures.is_some());

        // Par no registrado
        let buf_none = mgr.obtener("SOLUSDT", Mercado::Spot);
        assert!(buf_none.is_none());
    }

    #[tokio::test]
    async fn test_manager_insertar_trade() {
        let mut mgr = BufferManager::new();
        mgr.registrar("BTCUSDT", Mercado::Spot);

        let ts = ahora_ms();
        let trade = crear_trade(ts, 1000.0, SIDE_BUY);

        let ok = mgr.insertar_trade("BTCUSDT", Mercado::Spot, trade).await;
        assert!(ok);

        // Verificar que se insertó
        let buf = mgr.obtener("BTCUSDT", Mercado::Spot).unwrap();
        let guard = buf.read().await;
        assert_eq!(guard.len(), 1);
        assert!((guard.stats().volumen_total_usdt - 1000.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_manager_insertar_en_buffer_inexistente() {
        let mgr = BufferManager::new();

        let ts = ahora_ms();
        let trade = crear_trade(ts, 1000.0, SIDE_BUY);

        let ok = mgr.insertar_trade("BTCUSDT", Mercado::Spot, trade).await;
        assert!(!ok);
    }

    #[tokio::test]
    async fn test_manager_stats_todos() {
        let mut mgr = BufferManager::new();
        mgr.registrar("BTCUSDT", Mercado::Spot);
        mgr.registrar("BTCUSDT", Mercado::Futures);

        let ts = ahora_ms();
        mgr.insertar_trade("BTCUSDT", Mercado::Spot, crear_trade(ts, 100.0, SIDE_BUY))
            .await;
        mgr.insertar_trade(
            "BTCUSDT",
            Mercado::Futures,
            crear_trade(ts, 500.0, SIDE_SELL),
        )
        .await;

        let stats = mgr.stats_todos().await;
        assert_eq!(stats.len(), 2);
    }

    #[tokio::test]
    async fn test_manager_limpiar_todos() {
        let mut mgr = BufferManager::new();
        mgr.registrar("BTCUSDT", Mercado::Spot);
        mgr.registrar("BTCUSDT", Mercado::Futures);

        let ts = ahora_ms();
        mgr.insertar_trade("BTCUSDT", Mercado::Spot, crear_trade(ts, 100.0, SIDE_BUY))
            .await;
        mgr.insertar_trade(
            "BTCUSDT",
            Mercado::Futures,
            crear_trade(ts, 500.0, SIDE_SELL),
        )
        .await;

        mgr.limpiar_todos().await;

        let stats = mgr.stats_todos().await;
        for (_, s) in &stats {
            assert_eq!(s.total_trades, 0);
        }
    }

    #[tokio::test]
    async fn test_manager_no_duplicar_buffer() {
        let mut mgr = BufferManager::new();
        mgr.registrar("BTCUSDT", Mercado::Spot);
        mgr.registrar("BTCUSDT", Mercado::Spot); // Duplicado

        assert_eq!(mgr.cantidad(), 1); // Solo uno
    }
}
