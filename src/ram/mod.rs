// ═══════════════════════════════════════════════════════
// FORJA v1.0 - Módulo 4: Almacén en RAM (24 horas)
// ═══════════════════════════════════════════════════════
// Responsabilidad:
//   Mantener una ventana deslizante de 24 horas de trades
//   normalizados en memoria. Buffer circular que descarta
//   los trades más antiguos cuando se excede la ventana.
//
// Diseño:
//   - 4 buffers independientes:
//     1. Par 1 Spot    (ej: BTCUSDT spot)
//     2. Par 1 Futures (ej: BTCUSDT futures)
//     3. Par 2 Spot    (ej: SOLUSDT spot)
//     4. Par 2 Futures (ej: SOLUSDT futures)
//
//   - Cada buffer recibe trades sin bloquear a los demás
//   - Acceso concurrente seguro (lectura múltiple / escritura única)
//   - Purga automática de trades fuera de ventana de 24hrs
//   - Las comparaciones entre mercados se hacen sobre
//     acumuladores, NO sobre trades individuales
//
// Estimación de memoria:
//   BTC: ~2M trades/día × 33 bytes = ~63 MB
//   Par secundario: variable
//   Total estimado: < 200 MB para ambos pares
// ═══════════════════════════════════════════════════════

pub mod buffer;
pub mod analisis;
