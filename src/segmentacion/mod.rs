// ═══════════════════════════════════════════════════════
// FORJA v1.0 - Módulo 5: Motor de Segmentación
// ═══════════════════════════════════════════════════════
// Responsabilidad:
//   Clasificar cada trade individual por segmento de
//   actor según su volumen en USDT. Los rangos son
//   configurables por par desde archivo TOML.
//
// Segmentos por defecto (14):
//   P1  micro_retail      0 - 39.99
//   P2  retail_puro       40 - 99.99
//   P3  retail_pequeno    100 - 499.99
//   P4  retail_activo     500 - 999.99
//   P5  retail_plus       1,000 - 2,499.99
//   P6  bisagra_bajo      2,500 - 4,999.99
//   P7  bisagra_alto      5,000 - 9,999.99
//   P8  profesional       10,000 - 49,999.99
//   P9  institucional_bajo  50,000 - 99,999.99
//   P10 institucional_alto  100,000 - 499,999.99
//   P11 ballena_baby      500,000 - 999,999.99
//   P12 ballena_azul      1,000,000 - 4,999,999.99
//   P13 whale_evento      5,000,000 - 9,999,999.99
//   P14 market_maker      10,000,000+
//
// Perfiles de visualización:
//   14 segmentos: máxima resolución
//   7 segmentos:  resolución media (agrupaciones empíricas)
//   3 segmentos:  visión macro para decisión rápida
// ═══════════════════════════════════════════════════════

pub mod segmentacion;
