// ═══════════════════════════════════════════════════════
// FORJA v1.0 - Sistema de Inteligencia de Mercado Crypto
// ═══════════════════════════════════════════════════════

mod acumuladores;
mod config;
mod indicadores;
mod ingesta;
mod normalizador;
mod orderbook;
mod parquet_writer;
mod ram;
mod segmentacion;
mod sql;
mod ui;

use acumuladores::acumuladores::GestorAcumuladores;
use config::control::{
    CambioPendiente, ComandoSistema, SesionActual, SesionSnapshot,
    sesion_actual_utc, proximo_volcado_utc, segundos_hasta_proximo_volcado,
};
use indicadores::sr::{ResultadoSR, iniciar_scheduler_sr};
use ingesta::ingesta::{iniciar_ingesta, ConfigPar};
use normalizador::normalizador::Mercado;
use orderbook::orderbook::{M7State, iniciar_depth_stream, iniciar_liquidaciones_stream, resetear_liquidaciones};
use parquet_writer::escritor::{escribir_parquet, generar_ruta, TradeConSeg};
use ram::buffer::BufferManager;
use segmentacion::segmentacion::MotorSegmentacion;
use ui::servidor::{iniciar_servidor, AppState};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing_subscriber::EnvFilter;

const PUERTO_UI: u16 = 3000;
const DATA_DIR: &str = "data";

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_target(false)
        .with_timer(tracing_subscriber::fmt::time::ChronoLocal::new("%H:%M:%S%.3f".to_string()))
        .init();

    println!("══════════════════════════════════════════════════");
    println!("  ⚒ FORJA v1.0");
    println!("  Sistema de Inteligencia de Mercado Crypto");
    println!("══════════════════════════════════════════════════\n");

    let sistema_activo = Arc::new(RwLock::new(true));
    let par_activo = Arc::new(RwLock::new("SOLUSDT".to_string()));
    let precio_spot = Arc::new(RwLock::new(0.0f64));
    let cambio_pendiente = Arc::new(RwLock::new(CambioPendiente::default()));
    let (_, nombre_sesion) = sesion_actual_utc();
    let sesion_anterior = Arc::new(RwLock::new(SesionSnapshot::default()));
    let sesion_actual = Arc::new(RwLock::new(SesionActual::nueva(nombre_sesion)));
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<ComandoSistema>();
    let ultimo_sr: Arc<RwLock<Option<ResultadoSR>>> = Arc::new(RwLock::new(None));

    let mut par_corriendo = "SOLUSDT".to_string();

    loop {
        println!("  Par: {} | Sesión: {} | Volcado: {}", par_corriendo, nombre_sesion, proximo_volcado_utc());

        let motor = MotorSegmentacion::default_para(&par_corriendo);
        let mut buffer_mgr = BufferManager::new();
        buffer_mgr.registrar(&par_corriendo, Mercado::Spot);
        buffer_mgr.registrar(&par_corriendo, Mercado::Futures);
        let buffer_mgr = Arc::new(buffer_mgr);

        let pq_spot: Arc<RwLock<VecDeque<TradeConSeg>>> = Arc::new(RwLock::new(VecDeque::new()));
        let pq_futures: Arc<RwLock<VecDeque<TradeConSeg>>> = Arc::new(RwLock::new(VecDeque::new()));

        let acum_spot = Arc::new(RwLock::new(GestorAcumuladores::new(&format!("{}_SPOT", par_corriendo))));
        let acum_futures = Arc::new(RwLock::new(GestorAcumuladores::new(&format!("{}_FUTURES", par_corriendo))));

        // M7: Order Book + Liquidaciones
        let m7 = M7State::new();

        *precio_spot.write().await = 0.0;
        let (_, sesion_nombre) = sesion_actual_utc();
        *sesion_actual.write().await = SesionActual::nueva(sesion_nombre);
        *par_activo.write().await = par_corriendo.clone();
        *cambio_pendiente.write().await = CambioPendiente::default();
        resetear_liquidaciones(&m7.liquidaciones).await;

        // M9: Servidor UI (solo primera vez)
        static SERVIDOR_INICIADO: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        if !SERVIDOR_INICIADO.load(std::sync::atomic::Ordering::Relaxed) {
            SERVIDOR_INICIADO.store(true, std::sync::atomic::Ordering::Relaxed);
            let app_state = AppState {
                acum_spot: Arc::clone(&acum_spot), acum_futures: Arc::clone(&acum_futures),
                buffer_mgr: Arc::clone(&buffer_mgr), sistema_activo: Arc::clone(&sistema_activo),
                par_activo: Arc::clone(&par_activo), precio_spot: Arc::clone(&precio_spot),
                sesion_anterior: Arc::clone(&sesion_anterior), sesion_actual: Arc::clone(&sesion_actual),
                cambio_pendiente: Arc::clone(&cambio_pendiente), cmd_tx: cmd_tx.clone(),
                orderbook: Arc::clone(&m7.orderbook), liquidaciones: Arc::clone(&m7.liquidaciones),
                data_dir: DATA_DIR.to_string(),
                ultimo_sr: Arc::clone(&ultimo_sr),
            };
            tokio::spawn(async move { iniciar_servidor(app_state, PUERTO_UI).await; });
            println!("  UI Web: http://localhost:{}", PUERTO_UI);

            // M11: Scheduler S/R
            let sr_par = Arc::clone(&par_activo);
            let sr_ultimo = Arc::clone(&ultimo_sr);
            let sr_dir = DATA_DIR.to_string();
            tokio::spawn(async move {
                iniciar_scheduler_sr(sr_par, sr_dir, sr_ultimo).await;
            });
        }

        // M2: Ingesta trades
        let pares = vec![ConfigPar::new(&par_corriendo, true, true)];
        let (mut rx, stop_tx) = iniciar_ingesta(pares).await;

        // M7: Streams de depth y liquidaciones
        let ob_state = Arc::clone(&m7.orderbook);
        let liq_state = Arc::clone(&m7.liquidaciones);
        let depth_stop = stop_tx.subscribe();
        let liq_stop = stop_tx.subscribe();
        let depth_sym = par_corriendo.clone();
        let liq_sym = par_corriendo.clone();

        tokio::spawn(async move { iniciar_depth_stream(depth_sym, ob_state, depth_stop).await; });
        tokio::spawn(async move { iniciar_liquidaciones_stream(liq_sym, liq_state, liq_stop).await; });

        // Ctrl+C
        let stop_clone = stop_tx.clone();
        let activo_clone = Arc::clone(&sistema_activo);
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            *activo_clone.write().await = false;
            let _ = stop_clone.send(true);
        });

        println!("  ... capturando trades + depth + liquidaciones\n");

        // Scheduler volcado 8hrs
        let sched_pq_spot = Arc::clone(&pq_spot);
        let sched_pq_futures = Arc::clone(&pq_futures);
        let sched_sesion_ant = Arc::clone(&sesion_anterior);
        let sched_sesion_act = Arc::clone(&sesion_actual);
        let sched_cambio = Arc::clone(&cambio_pendiente);
        let sched_par = par_corriendo.clone();
        let sched_stop = stop_tx.clone();

        let sched_handle = tokio::spawn(async move {
            let espera = segundos_hasta_proximo_volcado();
            tracing::info!("SCHEDULER | Volcado en {}h {}m", espera/3600, (espera%3600)/60);
            tokio::time::sleep(tokio::time::Duration::from_secs(espera)).await;

            let snapshot = sched_sesion_act.read().await.generar_snapshot();
            tracing::info!("SCHEDULER | Sesión {} cerrada", snapshot.nombre);
            *sched_sesion_ant.write().await = snapshot;

            { let mut buf = sched_pq_spot.write().await;
              if !buf.is_empty() { let trades: Vec<TradeConSeg> = buf.drain(..).collect();
                let ruta = generar_ruta(DATA_DIR, &sched_par, "spot");
                match escribir_parquet(&trades, &ruta) { Ok(n) => tracing::info!("PARQUET | SPOT: {} trades", n), Err(e) => tracing::error!("PARQUET | SPOT: {}", e), }}}
            { let mut buf = sched_pq_futures.write().await;
              if !buf.is_empty() { let trades: Vec<TradeConSeg> = buf.drain(..).collect();
                let ruta = generar_ruta(DATA_DIR, &sched_par, "futures");
                match escribir_parquet(&trades, &ruta) { Ok(n) => tracing::info!("PARQUET | FUTURES: {} trades", n), Err(e) => tracing::error!("PARQUET | FUTURES: {}", e), }}}

            let cp = sched_cambio.read().await.clone();
            if cp.activo { let _ = sched_stop.send(true); }
            let (_, nombre) = sesion_actual_utc();
            *sched_sesion_act.write().await = SesionActual::nueva(nombre);
        });

        // Loop principal
        let mut contador: u64 = 0;
        let mut nuevo_par_solicitado: Option<String> = None;
        let mut stop_solicitado = false;

        loop {
            tokio::select! {
                trade_opt = rx.recv() => {
                    match trade_opt {
                        Some(evento) => {
                            contador += 1;
                            let seg = motor.segmentar(evento.trade);
                            buffer_mgr.insertar_trade(&evento.simbolo, evento.mercado,
                                crate::parquet_writer::escritor::TradeConSeg{trade:evento.trade,seg:seg.segment_id}).await;
                            match evento.mercado {
                                Mercado::Spot => {
                                    *precio_spot.write().await = evento.trade.price;
                                    acum_spot.write().await.procesar(&seg);
                                    sesion_actual.write().await.procesar_trade(&evento.trade, seg.segment_id);
                                    pq_spot.write().await.push_back(TradeConSeg { trade: evento.trade, seg: seg.segment_id });
                                }
                                Mercado::Futures => {
                                    acum_futures.write().await.procesar(&seg);
                                    pq_futures.write().await.push_back(TradeConSeg { trade: evento.trade, seg: seg.segment_id });
                                }
                            }
                            if contador <= 5 { println!("  #{:<4} {} {:<8} │ {} │ {:>10.2} USDT │ {}", contador, evento.mercado, evento.simbolo, seg.nombre_corto(), seg.trade.volume_usdt, seg.trade.side_str()); }
                            if contador == 6 { println!("  ... (http://localhost:{})", PUERTO_UI); }
                            if contador % 50000 == 0 { println!("  [{} trades] {} activo", contador, par_corriendo); }
                        }
                        None => break,
                    }
                }
                cmd_opt = cmd_rx.recv() => {
                    match cmd_opt {
                        Some(ComandoSistema::CambiarPar { nuevo_par }) => { tracing::info!("CONTROL | Cambio: {} → {}", par_corriendo, nuevo_par); nuevo_par_solicitado = Some(nuevo_par); }
                        Some(ComandoSistema::StopProgramado) => { tracing::info!("CONTROL | Stop programado"); stop_solicitado = true; }
                        None => {}
                    }
                }
            }
        }

        sched_handle.abort();

        if stop_solicitado || !*sistema_activo.read().await {
            println!("\n══════════════════════════════════════════════════");
            println!("  FORJA detenida. {} trades de {}.", contador, par_corriendo);
            println!("══════════════════════════════════════════════════");
            break;
        }
        if let Some(nuevo) = nuevo_par_solicitado {
            println!("\n  Cambiando {} → {}...", par_corriendo, nuevo);
            par_corriendo = nuevo;
            continue;
        }
        break;
    }
}
