# ⚒ FORJA v1.0
### Sistema de Inteligencia de Mercado Crypto en Tiempo Real

FORJA es una plataforma propietaria de análisis de mercado que reconstruye el comportamiento de actores institucionales y minoristas en tiempo real, revelando patrones invisibles en los charts convencionales.

---

## ¿Qué hace FORJA?

Mientras los traders convencionales leen velas y osciladores, FORJA disecciona **quién** está comprando y vendiendo en cada momento — segmentando cada trade por tamaño en 14 categorías de actores, desde micro retail hasta ballenas institucionales.

---

## Arquitectura

### Sistema 1 — FORJA (Rust, tiempo real)
- Ingesta tick-by-tick desde Binance WebSocket (spot + futures)
- Segmentación en 14 actores P1–P14 por volumen USDT
- Acumuladores deslizantes en 9 temporalidades: 3m, 5m, 15m, 1h, 2h, 4h, 8h, 12h, 24h
- Order Book futures depth20 con CVD inter-nivel por escalón de precio
- Volcado Parquet cada 8 horas (04/12/20 UTC)
- UI Web en tiempo real: Dashboard, Order Book, Liquidaciones, Indicadores, Parquet, SQL Lab

### Sistema 2 — DATAFORJ (Python, histórico)
- Descarga datos históricos desde data.binance.vision
- Mismo schema Parquet que FORJA — análisis intercambiable
- Permite reconstruir campañas institucionales pasadas

---

## Segmentación de Actores P1–P14

| Segmento | Rango USDT | Tipo de Actor |
|----------|-----------|---------------|
| P1 | < $40 | Micro retail |
| P2 | $40 – $100 | Retail pequeño |
| P3 | $100 – $500 | Retail |
| P4 | $500 – $1K | Retail pro |
| P5 | $1K – $2.5K | Retail pro |
| P6 | $2.5K – $5K | Bisagra |
| P7 | $5K – $10K | Bisagra |
| P8 | $10K – $50K | Market Maker |
| P9 | $50K – $100K | Institucional bajo |
| P10 | $100K – $500K | Institucional |
| P11 | $500K – $1M | Institucional alto |
| P12 | $1M – $5M | Ballena baja |
| P13 | $5M – $10M | Ballena media |
| P14 | > $10M | Ballena alta |

---

## Módulos

| Módulo | Descripción |
|--------|-------------|
| M1 | Control de sesiones (Asia/Londres/NY) |
| M2 | Ingesta WebSocket Binance |
| M3 | Normalizador de trades |
| M4 | Buffer RAM 24h (TradeConSeg) |
| M5 | Segmentación 14 actores |
| M6 | Acumuladores 9 temporalidades |
| M7 | Order Book futures + liquidaciones |
| M8 | Volcado Parquet 8h |
| M9 | UI Web tiempo real |
| M10 | SQL Lab (Polars Lazy) |
| M11 | S/R automático post-volcado |

---

## Indicadores propietarios

- **CVD inter-nivel** — diferencia buy/sell por escalón de precio en el book
- **S/R por segmento** — dónde cada actor defiende o abandona el precio
- **Divergencia Smart vs Retail** — separación de comportamiento por grupos de actores
- **VWAP por sesión** — precio promedio ponderado por volumen por sesión de mercado
- **RSI/MACD/VWAP por grupo** — indicadores técnicos aplicados a comportamiento de actores, no al precio

---

## Stack técnico

| Componente | Tecnología |
|------------|-----------|
| Core | Rust |
| Análisis | Polars Lazy |
| Almacenamiento | Parquet (Apache Arrow) |
| UI | Axum + HTML/JS vanilla |
| Datos históricos | Python + DuckDB |
| Fuente live | Binance WebSocket |
| Fuente histórica | data.binance.vision |

---

## Estado del proyecto

- ✅ FORJA v1.0 operativo — 11 módulos funcionando
- ✅ SQL Lab con análisis Parquet y RAM Live
- ✅ Segmentación 14 actores validada contra Binance
- ✅ Order Book validado (>99% precisión vs Binance)
- 🔄 Deploy cloud 24/7 (Hetzner) — pendiente
- 🔄 DATAFORJ — integración con schema FORJA en desarrollo
- 📋 EMAs por segmento — pendiente (requiere histórico acumulado)

---

## Por qué FORJA es diferente

Los sistemas existentes (Coinglass, Cignals, Nansen) ofrecen order flow genérico o análisis on-chain. Ninguno combina:

1. **14 segmentos de actores** por volumen USDT en tiempo real
2. **CVD por actor** — no solo el CVD global del mercado
3. **S/R por psicología de segmento** — dónde cada tipo de actor defiende o abandona
4. **Histórico propio acumulado** con el mismo schema que el tiempo real

---

*Construido desde cero con lógica propia. No es un indicador — es un microscopio de comportamiento de mercado.*
