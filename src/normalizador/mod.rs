// ═══════════════════════════════════════════════════════
// FORJA v1.0 - Módulo 3: Normalizador
// ═══════════════════════════════════════════════════════
// Responsabilidad:
//   Transformar el trade crudo de Binance al formato
//   interno de FORJA. Este es el ÚNICO punto del sistema
//   que conoce el formato de Binance. Si Binance cambia
//   su API, solo se modifica este módulo.
//
// Estructura normalizada: 33 bytes por trade
//   - timestamp:   u64  (8 bytes) - milisegundos UTC
//   - price:       f64  (8 bytes) - precio del trade
//   - volume_usdt: f64  (8 bytes) - volumen en USDT
//   - volume_base: f64  (8 bytes) - volumen en token base
//   - side:        u8   (1 byte)  - 0=Buy, 1=Sell
// ═══════════════════════════════════════════════════════

pub mod normalizador;
