// ═══════════════════════════════════════════════════════
// FORJA v1.0 - Módulo 2: Ingesta WebSocket
// ═══════════════════════════════════════════════════════
// Responsabilidad:
//   Conectar con Binance vía WebSocket y recibir trades
//   en tiempo real. Soporta spot y futures.
//
// Conexiones:
//   - Spot:    wss://stream.binance.com:9443/ws/<symbol>@trade
//   - Futures: wss://fstream.binance.com/ws/<symbol>@trade
//
// Características:
//   - Reconexión automática ante caídas
//   - Heartbeat para detectar desconexiones
//   - Cada trade recibido se pasa al Normalizador
// ═══════════════════════════════════════════════════════

pub mod ingesta;
