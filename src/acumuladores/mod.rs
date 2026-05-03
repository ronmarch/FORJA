// ═══════════════════════════════════════════════════════
// FORJA v1.0 - Módulo 6: Acumuladores por Temporalidad
// ═══════════════════════════════════════════════════════
// 9 temporalidades deslizantes, todas con la misma lógica:
//   3m, 5m, 15m, 1h, 2h, 4h, 8h, 12h, 24h
//
// - Se llenan una vez y deslizan indefinidamente
// - Solo se resetean con stop programado
// - Paquetes de 1 segundo como unidad base
// - Actualización O(1) por paquete
// ═══════════════════════════════════════════════════════

pub mod acumuladores;
