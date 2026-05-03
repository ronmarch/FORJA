// ═══════════════════════════════════════════════════════
// FORJA v1.0 - Servidor UI Web
// ═══════════════════════════════════════════════════════

use crate::acumuladores::acumuladores::{GestorAcumuladores, TOTAL_TEMPORALIDADES};
use crate::config::control::{SesionActual, SesionSnapshot, CambioPendiente, ComandoSistema, PARES_FAVORITOS, minutos_hasta_proximo_volcado, proximo_volcado_utc};
use crate::orderbook::orderbook::{OrderBookState, LiquidacionesState, ParCVD, VentanaLiq, CvdEscalonLiq};
use crate::ram::buffer::BufferManager;
use crate::ram::analisis::{AnalisisRAM, analizar_sr, analizar_cvd_ventanas, analizar_divergencia, analizar_indicadores_grupos, analizar_vwap_sesiones, VENTANAS_HORAS};
use crate::segmentacion::segmentacion::{NOMBRES_CORTOS, TOTAL_SEGMENTOS};
use crate::indicadores::sr::{ResultadoSR, MotorSR, ArchivoParquet};
use crate::sql::lab::{MotorAnalisis, ANALISIS_PREDEFINIDOS, AnalisisResult, ParquetInfo, listar_parquets_info};
use axum::{extract::State, response::Html, routing::{get, post}, Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

#[derive(Clone)]
pub struct AppState {
    pub acum_spot: Arc<RwLock<GestorAcumuladores>>,
    pub acum_futures: Arc<RwLock<GestorAcumuladores>>,
    pub buffer_mgr: Arc<BufferManager>,
    pub sistema_activo: Arc<RwLock<bool>>,
    pub par_activo: Arc<RwLock<String>>,
    pub precio_spot: Arc<RwLock<f64>>,
    pub sesion_anterior: Arc<RwLock<SesionSnapshot>>,
    pub sesion_actual: Arc<RwLock<SesionActual>>,
    pub cambio_pendiente: Arc<RwLock<CambioPendiente>>,
    pub cmd_tx: mpsc::UnboundedSender<ComandoSistema>,
    pub orderbook: Arc<RwLock<OrderBookState>>,
    pub liquidaciones: Arc<RwLock<LiquidacionesState>>,
    pub data_dir: String,
    pub ultimo_sr: Arc<RwLock<Option<ResultadoSR>>>,
}

// DTOs
#[derive(Serialize)] struct SegDTO{nombre:String,trades:u64,volumen:f64,pct_vol:f64,buy_pct:f64,sell_pct:f64}
#[derive(Serialize)] struct TempDTO{nombre:String,fill_pct:f64,trades:u64,volumen:f64,buy_pct:f64,sell_pct:f64}
#[derive(Serialize)] struct SesDTO{nombre:String,precio_apertura:f64,precio_cierre:f64,pd:String,pd_dir:String,volumen:f64,buy_pct:f64,sell_pct:f64,valido:bool}
#[derive(Serialize)] struct MktDTO{nombre:String,trades_total:u64,temporalidades:Vec<TempDTO>,segmentos:Vec<Vec<SegDTO>>}
#[derive(Serialize)] struct OBNivelDTO{precio:f64,cantidad:f64,usdt:f64}
#[derive(Serialize)] struct OBDTO{bids:Vec<OBNivelDTO>,asks:Vec<OBNivelDTO>,bid_total:f64,ask_total:f64,bid_pct:f64,ask_pct:f64,spread:f64,spread_pct:f64,mid_price:f64,cvd_global:f64,pares_cvd:Vec<ParCVD>}
#[derive(Serialize)] struct LiqDTO{timestamp:u64,precio:f64,usdt:f64,side:String,segmento:String}
#[derive(Serialize)] struct LiqSegDTO{nombre:String,long_count:u64,short_count:u64,long_usdt:f64,short_usdt:f64}
#[derive(Serialize)] struct LiqsDTO{recientes:Vec<LiqDTO>,por_segmento:Vec<LiqSegDTO>,total_long:f64,total_short:f64,total_count:u64,ventanas:Vec<VentanaLiq>,cvd_escalones:Vec<CvdEscalonLiq>}
#[derive(Serialize)] struct DashDTO{par:String,activo:bool,precio:f64,precio_futures:f64,secs_volcado:u64,pares_favoritos:Vec<String>,sesion_ant:SesDTO,sesion_act:SesDTO,cambio_pendiente:CambioPendiente,spot:MktDTO,futures:MktDTO,ob:OBDTO,liqs:LiqsDTO}
#[derive(Deserialize)] struct CambiarParReq{nuevo_par:String}
#[derive(Deserialize)] struct RamAnalizarReq{mercado:String,horas_indicadores:Option<u64>}
#[derive(Deserialize)] struct SrAnalizarReq{par:String,ruta:String}
#[derive(Deserialize)] struct SrExportarReq{resultado:ResultadoSR}
#[derive(Serialize)] struct SrArchivosRes{ok:bool,archivos:Vec<ArchivoParquet>}
#[derive(Serialize)] struct SrAnalizarRes{ok:bool,resultado:ResultadoSR}
#[derive(Serialize)] struct SrExportarRes{ok:bool,error:Option<String>}
#[derive(Serialize)] struct CambiarParRes{ok:bool,mensaje:String,minutos:u64,hora_volcado:String}

pub async fn iniciar_servidor(state: AppState, puerto: u16) {
    let app = Router::new()
        .route("/", get(pagina_principal))
        .route("/api/dashboard", get(api_dashboard))
        .route("/api/cambiar-par", post(api_cambiar_par))
        .route("/api/stop", post(api_stop))
        .route("/api/sql/queries", get(api_sql_queries))
        .route("/api/sql/ejecutar", post(api_sql_ejecutar))
        .route("/api/sql/pares", get(api_sql_pares))
        .route("/api/parquet/info", get(api_parquet_info))
        .route("/api/parquet/eliminar", post(api_parquet_eliminar))
        .route("/api/indicadores/sr", get(api_sr))
        .route("/api/ram/analizar", post(api_ram_analizar))
        .route("/api/sr/archivos", get(api_sr_archivos))
        .route("/api/sr/analizar", post(api_sr_analizar))
        .route("/api/sr/exportar", post(api_sr_exportar))
        .with_state(state);
    tracing::info!("UI WEB | http://localhost:{}", puerto);
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", puerto)).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn api_dashboard(State(s): State<AppState>) -> Json<DashDTO> {
    let par=s.par_activo.read().await.clone();
    let activo=*s.sistema_activo.read().await;
    let precio=*s.precio_spot.read().await;
    let spot=build_mkt(&s.acum_spot,"SPOT").await;
    let futures=build_mkt(&s.acum_futures,"FUTURES").await;
    let sa=s.sesion_anterior.read().await;
    let sesion_ant=SesDTO{nombre:sa.nombre.clone(),precio_apertura:sa.precio_apertura,precio_cierre:sa.precio_cierre,pd:sa.segmento_dominante.clone(),pd_dir:sa.pd_direccion.clone(),volumen:sa.volumen_total,buy_pct:sa.buy_pct,sell_pct:sa.sell_pct,valido:sa.valido};
    drop(sa);
    let sc=s.sesion_actual.read().await;
    let sesion_act=SesDTO{nombre:sc.nombre.clone(),precio_apertura:sc.precio_apertura,precio_cierre:sc.precio_actual,pd:sc.segmento_dominante.clone(),pd_dir:sc.pd_direccion.clone(),volumen:sc.volumen_total,buy_pct:sc.buy_pct,sell_pct:sc.sell_pct,valido:sc.trades_total>0};
    drop(sc);
    let cp=s.cambio_pendiente.read().await.clone();
    let pares_favoritos=PARES_FAVORITOS.iter().map(|p|p.to_string()).collect();

    // Order Book
    let ob_state=s.orderbook.read().await;
    let precio_ref=*s.precio_spot.read().await;
    let pares_cvd=ob_state.pares_cvd(precio_ref, 8);
    let cvd_global=ob_state.cvd_global();
    let ob=OBDTO{
        bids:ob_state.bids.iter().map(|n|OBNivelDTO{precio:n.precio,cantidad:n.cantidad,usdt:n.usdt}).collect(),
        asks:ob_state.asks.iter().map(|n|OBNivelDTO{precio:n.precio,cantidad:n.cantidad,usdt:n.usdt}).collect(),
        bid_total:ob_state.bid_total_usdt,ask_total:ob_state.ask_total_usdt,
        bid_pct:ob_state.bid_pct,ask_pct:ob_state.ask_pct,
        spread:ob_state.spread,spread_pct:ob_state.spread_pct,mid_price:ob_state.mid_price,
        cvd_global, pares_cvd,
    };
    let ob_state_precio = ob_state.mid_price;
    drop(ob_state);

    // Liquidaciones
    let lq=s.liquidaciones.read().await;
    let ventanas=lq.calcular_ventanas();
    let cvd_escalones=lq.cvd_escalones(1440);
    let liqs=LiqsDTO{
        recientes:lq.recientes.iter().rev().take(20).map(|l|LiqDTO{timestamp:l.timestamp,precio:l.precio,usdt:l.cantidad_usdt,side:l.side.clone(),segmento:l.segmento.clone()}).collect(),
        por_segmento:lq.por_segmento.iter().filter(|s|s.long_count>0||s.short_count>0).map(|s|LiqSegDTO{nombre:s.nombre.clone(),long_count:s.long_count,short_count:s.short_count,long_usdt:s.long_usdt,short_usdt:s.short_usdt}).collect(),
        total_long:lq.total_long_liq,total_short:lq.total_short_liq,total_count:lq.total_count,
        ventanas, cvd_escalones,
    };
    drop(lq);

    let precio_futures=ob_state_precio;
    let secs_volcado={
        use crate::config::control::minutos_hasta_proximo_volcado;
        minutos_hasta_proximo_volcado() * 60
    };
    Json(DashDTO{par,activo,precio,precio_futures,secs_volcado,pares_favoritos,sesion_ant,sesion_act,cambio_pendiente:cp,spot,futures,ob,liqs})
}

async fn api_cambiar_par(State(s):State<AppState>,Json(req):Json<CambiarParReq>)->Json<CambiarParRes>{
    let par_actual=s.par_activo.read().await.clone();let nuevo=req.nuevo_par.to_uppercase();
    if nuevo==par_actual{return Json(CambiarParRes{ok:false,mensaje:"Ya estás en este par".into(),minutos:0,hora_volcado:String::new()})}
    let mins=minutos_hasta_proximo_volcado();let hora=proximo_volcado_utc();
    {let mut cp=s.cambio_pendiente.write().await;*cp=CambioPendiente{activo:true,nuevo_par:nuevo.clone(),minutos_restantes:mins,hora_volcado:hora.clone(),es_stop:false};}
    let _=s.cmd_tx.send(ComandoSistema::CambiarPar{nuevo_par:nuevo.clone()});
    Json(CambiarParRes{ok:true,mensaje:format!("Cambio a {} programado",nuevo),minutos:mins,hora_volcado:hora})
}

async fn api_stop(State(s):State<AppState>)->Json<CambiarParRes>{
    let mins=minutos_hasta_proximo_volcado();let hora=proximo_volcado_utc();
    {let mut cp=s.cambio_pendiente.write().await;*cp=CambioPendiente{activo:true,nuevo_par:String::new(),minutos_restantes:mins,hora_volcado:hora.clone(),es_stop:true};}
    let _=s.cmd_tx.send(ComandoSistema::StopProgramado);
    Json(CambiarParRes{ok:true,mensaje:"Stop programado".into(),minutos:mins,hora_volcado:hora})
}

async fn build_mkt(acum:&Arc<RwLock<GestorAcumuladores>>,nombre:&str)->MktDTO{
    let a=acum.read().await;
    let mut temps=Vec::with_capacity(TOTAL_TEMPORALIDADES);
    for i in 0..TOTAL_TEMPORALIDADES{let t=a.temporalidad(i);let g=t.global();temps.push(TempDTO{nombre:t.nombre().to_string(),fill_pct:t.llenado_pct(),trades:g.trade_count,volumen:g.volume_total,buy_pct:g.pct_buy(),sell_pct:g.pct_sell()});}
    let mut segs=Vec::with_capacity(6);
    for idx in 0..TOTAL_TEMPORALIDADES{let t=a.temporalidad(idx);let g=t.global();let mut sv=Vec::new();
        for i in 0..TOTAL_SEGMENTOS{let seg=t.segmento((i+1)as u8);if seg.activo(){let pv=if g.volume_total>0.0{(seg.volume_total/g.volume_total)*100.0}else{0.0};sv.push(SegDTO{nombre:NOMBRES_CORTOS[i].to_string(),trades:seg.trade_count,volumen:seg.volume_total,pct_vol:pv,buy_pct:seg.pct_buy(),sell_pct:seg.pct_sell()});}}
        segs.push(sv);}
    MktDTO{nombre:nombre.to_string(),trades_total:a.trades_procesados(),temporalidades:temps,segmentos:segs}
}

#[derive(Serialize)] struct ParquetInfoResp { spot: Vec<ParquetInfo>, futures: Vec<ParquetInfo> }

#[derive(serde::Deserialize)] struct EliminarReq{nombre:String,mercado:String}
async fn api_parquet_eliminar(State(s):State<AppState>,Json(req):Json<EliminarReq>)->Json<serde_json::Value>{
    let par=s.par_activo.read().await.clone();
    let ruta=format!("{}/{}/{}/{}",&s.data_dir,par,req.mercado,req.nombre);
    match std::fs::remove_file(&ruta){
        Ok(())=>Json(serde_json::json!({"ok":true})),
        Err(e)=>Json(serde_json::json!({"ok":false,"error":e.to_string()})),
    }
}

async fn api_parquet_info(
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String,String>>,
    State(s):State<AppState>
)->Json<ParquetInfoResp>{
    let par=params.get("par").cloned().unwrap_or_else(||s.par_activo.try_read().map(|g|g.clone()).unwrap_or_else(|_|"SOLUSDT".to_string()));
    let spot_dir=format!("{}/{}/spot",&s.data_dir,par);
    let fut_dir=format!("{}/{}/futures",&s.data_dir,par);
    Json(ParquetInfoResp{
        spot: listar_parquets_info(&spot_dir),
        futures: listar_parquets_info(&fut_dir),
    })
}

#[derive(Deserialize)] struct AnalisisReq{id:String,par:String,mercado:String,archivo:Option<String>}
#[derive(Serialize)] struct AnalisisDTO{id:String,nombre:String,descripcion:String}

async fn api_sql_queries()->Json<Vec<AnalisisDTO>>{
    Json(ANALISIS_PREDEFINIDOS.iter().map(|a|AnalisisDTO{
        id:a.id.to_string(),nombre:a.nombre.to_string(),descripcion:a.descripcion.to_string()
    }).collect())
}

async fn api_sql_pares(State(s):State<AppState>)->Json<Vec<String>>{
    let motor=MotorAnalisis::new(&s.data_dir);
    Json(motor.listar_pares())
}

async fn api_sql_ejecutar(State(s):State<AppState>,Json(req):Json<AnalisisReq>)->Json<AnalisisResult>{
    let motor=MotorAnalisis::new(&s.data_dir);
    let archivo=req.archivo.clone().unwrap_or_else(||"all".to_string());
    let result=tokio::task::spawn_blocking(move||motor.ejecutar(&req.id,&req.par,&req.mercado,&archivo))
        .await.unwrap_or_else(|e|AnalisisResult{error:Some(format!("Error: {}",e)),..Default::default()});
    Json(result)
}

async fn api_sr(State(s):State<AppState>)->Json<Option<ResultadoSR>>{
    Json(s.ultimo_sr.read().await.clone())
}

async fn api_ram_analizar(
    State(s): State<AppState>,
    Json(req): Json<RamAnalizarReq>,
) -> Json<AnalisisRAM> {
    use crate::normalizador::normalizador::Mercado;
    let par     = s.par_activo.read().await.clone();
    let mercado = if req.mercado == "futures" { Mercado::Futures } else { Mercado::Spot };
    let horas_ind = req.horas_indicadores.unwrap_or(4);
    let precio_ref = *s.precio_spot.read().await;

    let buf_arc = s.buffer_mgr.obtener(&par, mercado);
    if buf_arc.is_none() {
        return Json(AnalisisRAM { mercado: req.mercado, precio_ref, ..Default::default() });
    }
    let buf_arc = buf_arc.unwrap();
    let buf     = buf_arc.read().await;
    let todos: Vec<_> = buf.todos_los_trades().iter().collect();
    let total   = todos.len();

    // S/R por todas las ventanas
    let sr_ventanas = VENTANAS_HORAS.iter().map(|&h| {
        analizar_sr(todos.iter().copied(), h)
    }).collect();

    // CVD ventanas
    let cvd_ventanas = analizar_cvd_ventanas(todos.iter().copied());

    // Divergencia ventanas
    let divergencia = analizar_divergencia(todos.iter().copied());

    // Indicadores grupos (ventana configurable)
    let todos2: Vec<_> = buf.todos_los_trades().iter().collect();
    let indicadores_grupos = analizar_indicadores_grupos(
        || todos2.iter().copied().collect(),
        horas_ind
    );

    // VWAP sesiones
    let vwap_sesiones = analizar_vwap_sesiones(todos.iter().copied(), precio_ref);

    Json(AnalisisRAM {
        mercado: req.mercado,
        total_trades_ram: total,
        precio_ref,
        sr_ventanas,
        cvd_ventanas,
        divergencia,
        indicadores_grupos,
        vwap_sesiones,
    })
}

async fn api_sr_archivos(
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String,String>>,
    State(s): State<AppState>,
) -> Json<SrArchivosRes> {
    let par=params.get("par").cloned().unwrap_or_else(||s.par_activo.try_read().map(|g|g.clone()).unwrap_or_else(|_|"SOLUSDT".to_string()));
    let data_dir=s.data_dir.clone();
    let archivos=tokio::task::spawn_blocking(move||MotorSR::new(&data_dir).listar_archivos(&par)).await.unwrap_or_default();
    Json(SrArchivosRes{ok:true,archivos})
}

async fn api_sr_analizar(State(s):State<AppState>,Json(req):Json<SrAnalizarReq>)->Json<SrAnalizarRes>{
    let data_dir=s.data_dir.clone();
    let par=req.par.clone(); let ruta=req.ruta.clone();
    let resultado=tokio::task::spawn_blocking(move||MotorSR::new(&data_dir).analizar_ruta(&ruta,&par))
        .await.unwrap_or_else(|e|ResultadoSR{error:Some(format!("spawn: {}",e)),..Default::default()});
    if resultado.error.is_none(){ *s.ultimo_sr.write().await=Some(resultado.clone()); }
    let ok=resultado.error.is_none();
    Json(SrAnalizarRes{ok,resultado})
}

async fn api_sr_exportar(State(s):State<AppState>,Json(req):Json<SrExportarReq>)->Json<SrExportarRes>{
    let data_dir=s.data_dir.clone(); let resultado=req.resultado;
    match tokio::task::spawn_blocking(move||MotorSR::new(&data_dir).guardar_resultado(&resultado)).await{
        Ok(Ok(()))=>Json(SrExportarRes{ok:true,error:None}),
        Ok(Err(e))=>Json(SrExportarRes{ok:false,error:Some(e)}),
        Err(e)=>Json(SrExportarRes{ok:false,error:Some(e.to_string())}),
    }
}

async fn pagina_principal()->Html<String>{Html(HTML_TEMPLATE.to_string())}

const HTML_TEMPLATE: &str = r##"<!DOCTYPE html>
<html lang="es"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"><title>FORJA v1.0</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}body{background:#1A1A1A;color:#D4D4D4;font-family:'Courier New',monospace;font-size:13px}
.header{background:#111;padding:8px 20px;display:flex;justify-content:space-between;align-items:center;border-bottom:2px solid #C9A84C}.header h1{color:#C9A84C;font-size:20px;letter-spacing:2px}.header-right{display:flex;align-items:center;gap:12px}.precio-box .precio{color:#FFF;font-size:22px;font-weight:bold}
.par-selector{background:#1A1A1A;color:#E6C35C;border:1px solid #C9A84C;padding:4px 8px;font-family:'Courier New',monospace;font-size:14px;cursor:pointer;border-radius:3px;font-weight:bold}.par-selector:focus{outline:none;border-color:#E6C35C}
.status{padding:3px 10px;border-radius:4px;font-size:11px}.status.on{background:#2d4a2d;color:#4CAF50}.status.off{background:#4a2d2d;color:#CF6679}
.btn-stop{background:#4a2d2d;color:#CF6679;border:1px solid #CF6679;padding:3px 10px;border-radius:4px;font-size:11px;cursor:pointer;font-family:'Courier New',monospace}.btn-stop:hover{background:#5a3d3d}
.session-bar{background:#222;padding:5px 20px;display:flex;gap:20px;font-size:12px;border-bottom:1px solid #333}.session-bar span{color:#B0B0B0}
.tabs{display:flex;background:#111;border-bottom:1px solid #333;overflow-x:auto;-webkit-overflow-scrolling:touch;scrollbar-width:none}.tabs::-webkit-scrollbar{display:none}.tab{padding:10px 18px;cursor:pointer;color:#B0B0B0;border-bottom:2px solid transparent;transition:0.2s;white-space:nowrap;flex-shrink:0}.tab:hover{color:#D4D4D4}.tab.active{color:#C9A84C;border-bottom:2px solid #C9A84C}
.content{padding:12px 20px}.panel{display:none}.panel.active{display:block}.grid{display:grid;grid-template-columns:1fr 1fr;gap:15px}
table{width:100%;border-collapse:collapse;margin:6px 0}th{background:#2C2418;color:#C9A84C;padding:5px 8px;text-align:left;font-size:11px;border:1px solid #3a3020}td{padding:4px 8px;border:1px solid #2a2a2a;font-size:12px}tr:nth-child(even){background:#1e1e1e}tr:hover{background:#252520}
.buy{color:#E6C35C}.sell{color:#B0B0B0}.muted{color:#666}.long-liq{color:#CF6679}.short-liq{color:#4CAF50}
.section-title{color:#C9A84C;font-size:13px;margin:10px 0 4px 0;padding-bottom:3px;border-bottom:1px solid #333;display:flex;align-items:center;gap:8px}
.tf-sel{background:#1A1A1A;color:#C9A84C;border:1px solid #C9A84C;padding:2px 6px;font-family:'Courier New',monospace;font-size:12px;cursor:pointer;border-radius:3px}
.stats-row{display:flex;gap:15px;margin:8px 0;flex-wrap:wrap}.stat-box{background:#222;padding:8px 14px;border-radius:4px;border:1px solid #333}.stat-box .label{color:#B0B0B0;font-size:10px}.stat-box .value{color:#E6C35C;font-size:16px;font-weight:bold}
.pending{text-align:center;padding:40px;color:#666}
.dot{width:8px;height:8px;border-radius:50%;display:inline-block;background:#4CAF50;animation:pulse 1s infinite}@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.3}}
.modal-bg{display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.7);z-index:100;justify-content:center;align-items:center}.modal-bg.show{display:flex}
.modal{background:#222;border:2px solid #C9A84C;border-radius:8px;padding:30px;max-width:450px;width:90%;text-align:center}.modal h2{color:#C9A84C;margin-bottom:15px}.modal p{margin:8px 0;font-size:13px}.modal .highlight{color:#E6C35C;font-size:18px;font-weight:bold}
.modal-btns{display:flex;gap:12px;justify-content:center;margin-top:20px}.modal-btns button{padding:8px 24px;border-radius:4px;font-family:'Courier New',monospace;font-size:13px;cursor:pointer;border:1px solid}
.btn-confirm{background:#2d4a2d;color:#4CAF50;border-color:#4CAF50}.btn-cancel{background:#333;color:#B0B0B0;border-color:#666}
.cambio-banner{background:#2C2418;border:1px solid #D4A017;padding:6px 20px;font-size:12px;color:#D4A017;display:none;text-align:center}.cambio-banner.show{display:block}
.ob-bar{height:24px;display:flex;border-radius:3px;overflow:hidden;margin:8px 0}.ob-bar-bid{background:#2d4a2d;display:flex;align-items:center;justify-content:center;color:#4CAF50;font-size:11px;font-weight:bold}.ob-bar-ask{background:#4a2d2d;display:flex;align-items:center;justify-content:center;color:#CF6679;font-size:11px;font-weight:bold}
.ob-depth{width:100%;border-collapse:collapse;font-family:'Courier New',monospace;font-size:12px;margin:6px 0}
.ob-depth th{background:#1a1408;color:#C9A84C;padding:4px 8px;text-align:center;font-size:10px;border:1px solid #2a2a2a}
.ob-depth td{padding:3px 8px;border:1px solid #1a1a1a}
.ob-depth .td-ask{color:#CF6679;text-align:right}.ob-depth .td-bid{color:#4CAF50;text-align:left}
.ob-depth .td-ap{color:#CF6679;font-weight:bold;text-align:center}.ob-depth .td-bp{color:#4CAF50;font-weight:bold;text-align:center}
.ob-depth .td-cvd{text-align:center;font-weight:bold;font-size:12px;min-width:100px}
.ob-depth .cvd-pos{color:#CF6679}.ob-depth .cvd-neg{color:#4CAF50}.ob-depth .cvd-neu{color:#666}
.ob-depth tr.precio-actual td{background:#1e1a0a;border-top:1px solid #C9A84C;border-bottom:1px solid #C9A84C}
.ob-depth tr.totales-row td{background:#111;border-top:2px solid #333}
.sr-toolbar{display:flex;align-items:center;gap:8px;padding:8px 0 10px 0;flex-wrap:wrap}
.sr-sel{background:#1a1a1a;color:#B0B0B0;border:1px solid #333;padding:4px 8px;font-family:'Courier New',monospace;font-size:12px;border-radius:3px;min-width:200px}.sr-sel:focus{outline:none;border-color:#C9A84C}
.btn-sr{background:#1e1e1e;color:#C9A84C;border:1px solid #C9A84C;padding:5px 14px;font-family:'Courier New',monospace;font-size:12px;border-radius:3px;cursor:pointer}.btn-sr:hover{background:#2C2418}.btn-sr:disabled{opacity:.4;cursor:default}
.btn-sr-exp{color:#4CAF50;border-color:#4CAF50}.btn-sr-exp:hover{background:#0d200d}
.sr-status{font-family:'Courier New',monospace;font-size:11px;color:#666}
@keyframes destello{0%,100%{opacity:1}50%{opacity:0.35}}
@keyframes glow-green{0%,100%{box-shadow:0 0 4px #4CAF5088}50%{box-shadow:0 0 10px #4CAF50cc}}
@keyframes glow-red{0%,100%{box-shadow:0 0 4px #CF667988}50%{box-shadow:0 0 10px #CF6679cc}}
.destello-verde{animation:destello 1.2s ease-in-out infinite;color:#4CAF50!important;font-weight:bold}
.destello-rojo{animation:destello 1.2s ease-in-out infinite;color:#CF6679!important;font-weight:bold}
.destello-ambar{animation:destello 1.4s ease-in-out infinite;color:#D4A017!important;font-weight:bold}
.badge-dom{display:inline-block;padding:1px 6px;border-radius:3px;font-size:10px;font-weight:bold;animation-duration:2s;animation-iteration-count:infinite;animation-timing-function:ease-in-out}
.badge-g1{background:#4CAF5033;color:#4CAF50;border:1px solid #4CAF5066;animation-name:glow-green}
.badge-g2{background:#4CAF5022;color:#4CAF50cc;border:1px solid #4CAF5044;animation-name:glow-green}
.badge-g3{background:#4CAF5011;color:#4CAF5099;border:1px solid #4CAF5033;animation-name:glow-green}
.badge-r1{background:#CF667933;color:#CF6679;border:1px solid #CF667966;animation-name:glow-red}
.badge-r2{background:#CF667922;color:#CF6679cc;border:1px solid #CF667944;animation-name:glow-red}
.badge-r3{background:#CF667911;color:#CF667999;border:1px solid #CF667933;animation-name:glow-red}
.precio-futures{font-size:13px;color:#888;font-family:'Courier New',monospace}
.countdown{font-family:'Courier New',monospace;font-size:11px;color:#555;padding:2px 8px;border:1px solid #333;border-radius:3px;background:#111}
</style></head><body>
<div class="header"><h1>⚒ FORJA <span style="font-size:11px;color:#666">v1.0</span></h1>
<div class="header-right"><select class="par-selector" id="par-sel" onchange="onParChange()"></select><div class="precio-box"><div class="precio" id="precio">--</div><div class="precio-futures">F: <span id="precio-fut">--</span></div></div><span class="countdown" id="countdown-volcado">volcado --:--</span><span class="status on" id="status">ACTIVO</span><button class="btn-stop" onclick="onStop()">STOP</button><span class="dot"></span></div></div>
<div class="cambio-banner" id="cambio-banner"></div>
<div class="session-bar"><span id="si">Anterior: --</span><span style="color:#333">│</span><span id="sp" style="color:#E6C35C">Actual: --</span><span style="margin-left:auto" id="utc">UTC: --</span></div>
<div class="tabs">
<div class="tab active" onclick="showTab('dashboard',this)">Dashboard</div>
<div class="tab" onclick="showTab('orderbook',this)">Order Book</div>
<div class="tab" onclick="showTab('liquidaciones',this)">Liquidaciones</div>
<div class="tab" onclick="showTab('indicadores',this)">Indicadores</div>
<div class="tab" onclick="showTab('parquet',this)">Parquet</div>
<div class="tab" onclick="showTab('sql',this)">SQL Lab</div>
</div>
<div class="content">
<!-- DASHBOARD -->
<div id="dashboard" class="panel active">
<div class="stats-row"><div class="stat-box"><div class="label">Trades Spot</div><div class="value" id="ts">0</div></div><div class="stat-box"><div class="label">Trades Futures</div><div class="value" id="tf">0</div></div><div class="stat-box"><div class="label">Total Trades</div><div class="value" id="tt">0</div></div></div>
<div class="grid"><div><div class="section-title">SPOT - Temporalidades</div><table><tr><th>TF</th><th>Fill</th><th>Trades</th><th>Vol USDT</th><th>Buy%</th><th>Sell%</th></tr><tbody id="st"></tbody></table>
<div class="section-title">SPOT - Segmentos <select class="tf-sel" id="ss-tf" onchange="stc()"><option value="0">3m</option><option value="1">5m</option><option value="2">15m</option><option value="3">1h</option><option value="4">2h</option><option value="5">4h</option><option value="6">8h</option><option value="7">12h</option><option value="8">24h</option></select></div>
<table><tr><th>Seg</th><th>Trades</th><th>Vol USDT</th><th>%Vol</th><th>Buy%</th><th>Sell%</th></tr><tbody id="ss"></tbody></table></div>
<div><div class="section-title">FUTURES - Temporalidades</div><table><tr><th>TF</th><th>Fill</th><th>Trades</th><th>Vol USDT</th><th>Buy%</th><th>Sell%</th></tr><tbody id="ft"></tbody></table>
<div class="section-title">FUTURES - Segmentos <select class="tf-sel" id="fs-tf" onchange="stc()"><option value="0">3m</option><option value="1">5m</option><option value="2">15m</option><option value="3">1h</option><option value="4">2h</option><option value="5">4h</option><option value="6">8h</option><option value="7">12h</option><option value="8">24h</option></select></div>
<table><tr><th>Seg</th><th>Trades</th><th>Vol USDT</th><th>%Vol</th><th>Buy%</th><th>Sell%</th></tr><tbody id="fs"></tbody></table></div></div></div>

<!-- ORDER BOOK -->
<div id="orderbook" class="panel">
<div class="stats-row">
<div class="stat-box"><div class="label">Bid Total</div><div class="value buy" id="ob-bid-total">--</div></div>
<div class="stat-box"><div class="label">Ask Total</div><div class="value sell" id="ob-ask-total">--</div></div>
<div class="stat-box"><div class="label">Spread</div><div class="value" id="ob-spread">--</div></div>
<div class="stat-box"><div class="label">Mid Price</div><div class="value" id="ob-mid">--</div></div>
<div class="stat-box"><div class="label">CVD Global</div><div class="value" id="ob-cvd-badge">--</div></div>
</div>
<div class="section-title">Ratio Bid/Ask — 8 escalones desde precio actual</div>
<div class="ob-bar"><div class="ob-bar-bid" id="ob-bar-bid" style="width:50%">50%</div><div class="ob-bar-ask" id="ob-bar-ask" style="width:50%">50%</div></div>
<div class="section-title">Profundidad por escalones enteros + CVD inter-nivel</div>
<table class="ob-depth">
<thead><tr>
  <th style="text-align:right">Vol USDT (Ask)</th><th>$ Ask</th>
  <th>CVD escalón</th>
  <th>$ Bid</th><th style="text-align:left">Vol USDT (Bid)</th>
</tr></thead>
<tbody id="ob-depth-body"></tbody>
<tfoot>
<tr class="precio-actual"><td colspan="5" style="text-align:center;color:#C9A84C;font-weight:bold" id="ob-precio-actual">— precio actual —</td></tr>
<tr class="totales-row">
  <td id="ob-ask-total-pie" style="text-align:right;color:#CF6679;font-weight:bold;padding:4px 8px">--</td>
  <td style="text-align:center;color:#666;font-size:10px">Total Ask</td>
  <td id="ob-cvd-global" style="text-align:center;font-weight:bold;font-size:13px">--</td>
  <td style="text-align:center;color:#666;font-size:10px">Total Bid</td>
  <td id="ob-bid-total-pie" style="text-align:left;color:#4CAF50;font-weight:bold;padding:4px 8px">--</td>
</tr>
</tfoot>
</table>
</div>

<!-- LIQUIDACIONES -->
<div id="liquidaciones" class="panel">
<div class="stats-row">
<div class="stat-box"><div class="label">Total Liq</div><div class="value" id="liq-total">0</div></div>
<div class="stat-box"><div class="label">Long Liq</div><div class="value long-liq" id="liq-long">$0</div></div>
<div class="stat-box"><div class="label">Short Liq</div><div class="value short-liq" id="liq-short">$0</div></div>
<div class="stat-box"><div class="label">Ventana CVD</div>
  <select class="tf-sel" id="liq-ventana-sel" onchange="liqCambiarVentana(this.value)">
    <option value="15">15m</option><option value="60">1h</option>
    <option value="120">2h</option><option value="240">4h</option>
    <option value="1440" selected>24h</option>
  </select>
</div>
</div>
<div class="section-title">Ventanas Temporales</div>
<table><tr><th>Ventana</th><th>Long $</th><th>Short $</th><th>Total</th><th>Dominancia</th></tr><tbody id="liq-ventanas"></tbody></table>
<div class="section-title">CVD de Liquidaciones por Escalón de Precio <span style="color:#666;font-size:10px">(pos=más longs destruidos · neg=más shorts destruidos)</span></div>
<div id="liq-cvd-escalones" style="font-family:'Courier New',monospace;font-size:12px;margin:4px 0"></div>
<div class="section-title" style="margin-top:10px">Liquidaciones por Segmento</div>
<table><tr><th>Seg</th><th>Long Liq</th><th>Long $</th><th>Short Liq</th><th>Short $</th></tr><tbody id="liq-seg"></tbody></table>
<div class="section-title">Liquidaciones Recientes</div>
<table><tr><th>Hora</th><th>Tipo</th><th>Segmento</th><th>USDT</th><th>Precio</th></tr><tbody id="liq-rec"></tbody></table>
</div>

<!-- INDICADORES -->
<div id="indicadores" class="panel">
<div class="sr-toolbar">
  <select id="sr-mercado" class="sr-sel" style="min-width:90px" onchange="srCargarArchivos()"><option value="spot">Spot</option><option value="futures">Futures</option></select>
  <select id="sr-archivo" class="sr-sel"><option value="">— Seleccionar Parquet —</option></select>
  <button class="btn-sr" id="btn-sr-analizar" onclick="srAnalizar()">▶ Analizar</button>
  <button class="btn-sr btn-sr-exp" id="btn-sr-exportar" onclick="srExportar()" style="display:none">↓ Guardar Parquet</button>
  <span class="sr-status" id="sr-status"></span>
</div>
<div class="stats-row">
  <div class="stat-box"><div class="label">Último análisis</div><div class="value" id="sr-ts" style="font-size:12px;color:#B0B0B0">--</div></div>
  <div class="stat-box"><div class="label">Sesión</div><div class="value" id="sr-sesion">--</div></div>
  <div class="stat-box"><div class="label">Precio ref</div><div class="value" id="sr-precio">--</div></div>
  <div class="stat-box"><div class="label">Bucket</div><div class="value" id="sr-bucket" style="font-size:13px">--</div></div>
</div>
<div class="grid">
<div>
  <div class="section-title" style="color:#CF6679">▲ Resistencias - Top 3 por Volumen Sell</div>
  <table><tr><th>Rank</th><th>Precio</th><th>Vol Sell</th><th>Buy%</th><th>Sell%</th><th>Seg Dom</th><th>Score</th></tr><tbody id="sr-rv"></tbody></table>
  <div class="section-title" style="color:#CF6679;margin-top:12px">▲ Resistencias - Top 3 por Trades</div>
  <table><tr><th>Rank</th><th>Precio</th><th>Trades</th><th>Buy%</th><th>Sell%</th><th>Seg Dom</th></tr><tbody id="sr-rt"></tbody></table>
</div>
<div>
  <div class="section-title" style="color:#4CAF50">▼ Soportes - Top 3 por Volumen Buy</div>
  <table><tr><th>Rank</th><th>Precio</th><th>Vol Buy</th><th>Buy%</th><th>Sell%</th><th>Seg Dom</th><th>Score</th></tr><tbody id="sr-sv"></tbody></table>
  <div class="section-title" style="color:#4CAF50;margin-top:12px">▼ Soportes - Top 3 por Trades</div>
  <table><tr><th>Rank</th><th>Precio</th><th>Trades</th><th>Buy%</th><th>Sell%</th><th>Seg Dom</th></tr><tbody id="sr-st"></tbody></table>
</div>
</div>
<div class="pending" id="sr-pending" style="padding:20px"><p style="color:#666">Selecciona un Parquet y pulsa ▶ Analizar</p></div>
</div>

<div id="parquet" class="panel">
<div class="stats-row">
  <div class="stat-box"><div class="label">Próximo volcado</div><div class="value" id="pq-next" style="font-size:13px">--</div></div>
  <div class="stat-box"><div class="label">Archivos Spot</div><div class="value" id="pq-spot-count">0</div></div>
  <div class="stat-box"><div class="label">Archivos Futures</div><div class="value" id="pq-fut-count">0</div></div>
  <div class="stat-box"><button onclick="pqRefresh()" style="background:#2C2418;color:#C9A84C;border:1px solid #C9A84C;padding:6px 14px;cursor:pointer;font-family:'Courier New',monospace;border-radius:3px">↻ Refrescar</button></div>
</div>
<div class="grid">
<div><div class="section-title">Spot - Archivos Parquet</div><table><tr><th>Archivo</th><th>Tamaño</th><th></th></tr><tbody id="pq-spot-list"></tbody></table></div>
<div><div class="section-title">Futures - Archivos Parquet</div><table><tr><th>Archivo</th><th>Tamaño</th><th></th></tr><tbody id="pq-fut-list"></tbody></table></div>
</div>
</div>

<div id="sql" class="panel">
<!-- Selector modo -->
<div style="display:flex;gap:0;margin-bottom:8px;border:1px solid #333;border-radius:4px;overflow:hidden;width:fit-content">
  <button id="btn-modo-parquet" onclick="sqlSetModo('parquet')" style="padding:6px 18px;background:#2C2418;color:#C9A84C;border:none;font-family:'Courier New',monospace;font-size:12px;cursor:pointer;border-right:1px solid #333">▦ Parquet</button>
  <button id="btn-modo-ram" onclick="sqlSetModo('ram')" style="padding:6px 18px;background:#111;color:#666;border:none;font-family:'Courier New',monospace;font-size:12px;cursor:pointer">⚡ RAM Live</button>
</div>

<!-- PARQUET MODE -->
<div id="sql-parquet-mode">
<div class="stats-row">
  <div class="stat-box"><div class="label">Par</div><select class="tf-sel" id="sql-par" style="font-size:13px;padding:4px 8px" onchange="sqlCargarArchivos()"></select></div>
  <div class="stat-box"><div class="label">Mercado</div><select class="tf-sel" id="sql-mkt" style="font-size:13px;padding:4px 8px" onchange="sqlCargarArchivos()"><option value="spot">Spot</option><option value="futures">Futures</option></select></div>
  <div class="stat-box" style="display:flex;flex-direction:column;gap:4px">
    <div class="label">Selección</div>
    <div style="display:flex;gap:4px">
      <button id="sql-modo-all"    onclick="sqlSetArchivo('all')"    style="padding:3px 8px;font-family:'Courier New',monospace;font-size:11px;cursor:pointer;background:#2C2418;color:#C9A84C;border:1px solid #C9A84C;border-radius:3px">Todos</button>
      <button id="sql-modo-rango"  onclick="sqlSetArchivo('rango')"  style="padding:3px 8px;font-family:'Courier New',monospace;font-size:11px;cursor:pointer;background:#111;color:#666;border:1px solid #333;border-radius:3px">Rango</button>
      <button id="sql-modo-uno"    onclick="sqlSetArchivo('uno')"    style="padding:3px 8px;font-family:'Courier New',monospace;font-size:11px;cursor:pointer;background:#111;color:#666;border:1px solid #333;border-radius:3px">Individual</button>
    </div>
    <div id="sql-sel-all" style="font-size:11px;color:#666">Escanea todos los archivos disponibles</div>
    <div id="sql-sel-rango" style="display:none;gap:4px;flex-direction:column">
      <select class="tf-sel" id="sql-desde" style="font-size:11px;padding:3px 6px"></select>
      <select class="tf-sel" id="sql-hasta" style="font-size:11px;padding:3px 6px"></select>
    </div>
    <div id="sql-sel-uno" style="display:none">
      <select class="tf-sel" id="sql-archivo-uno" style="font-size:11px;padding:3px 6px;min-width:220px"></select>
    </div>
  </div>
  <div class="stat-box" style="flex:1"><div class="label">Estado</div><div class="value" id="sql-info" style="font-size:12px;color:#B0B0B0">Selecciona un análisis</div></div>
  <div class="stat-box"><button onclick="sqlExportar()" style="background:#222;color:#B0B0B0;border:1px solid #666;padding:6px 16px;cursor:pointer;font-family:'Courier New',monospace;border-radius:3px">↓ CSV</button></div>
</div>
<div class="section-title">Análisis disponibles (Polars Lazy)</div>
<div id="sql-predef" style="display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-bottom:12px"></div>
<div class="section-title">Resultados <span id="sql-tiempo" style="color:#666;font-size:11px"></span></div>
<div style="overflow-x:auto"><table style="min-width:100%"><thead id="sql-thead"></thead><tbody id="sql-tbody"></tbody></table></div>
</div>

<!-- RAM MODE -->
<div id="sql-ram-mode" style="display:none">
<div class="stats-row">
  <div class="stat-box"><div class="label">Mercado RAM</div>
    <select class="tf-sel" id="ram-mkt" style="font-size:13px;padding:4px 8px">
      <option value="spot">Spot</option><option value="futures">Futures</option>
    </select>
  </div>
  <div class="stat-box"><div class="label">Indicadores TF</div>
    <select class="tf-sel" id="ram-horas-ind" style="font-size:13px;padding:4px 8px">
      <option value="1">1h</option><option value="2">2h</option>
      <option value="4" selected>4h</option><option value="8">8h</option>
    </select>
  </div>
  <div class="stat-box"><button onclick="ramAnalizar()" class="btn-sr">⚡ Analizar RAM</button></div>
  <div class="stat-box" style="flex:1"><div class="label">Estado</div><div id="ram-info" style="font-size:12px;color:#B0B0B0">Listo</div></div>
</div>

<!-- S/R por ventanas -->
<div class="section-title">Soportes y Resistencias — Ventanas Temporales (RAM)</div>
<div id="ram-sr-tabs" style="display:flex;gap:4px;margin-bottom:6px;flex-wrap:wrap"></div>
<div class="grid">
<div>
  <div class="section-title" style="color:#CF6679">▲ Resistencias — Vol Sell</div>
  <table><tr><th>Rank</th><th>Precio</th><th>Vol Sell</th><th>Buy%</th><th>Sell%</th><th>Seg Dom</th><th>Score</th></tr><tbody id="ram-rv"></tbody></table>
  <div class="section-title" style="color:#CF6679;margin-top:8px">▲ Resistencias — Trades</div>
  <table><tr><th>Rank</th><th>Precio</th><th>Trades</th><th>Buy%</th><th>Sell%</th><th>Seg Dom</th></tr><tbody id="ram-rt"></tbody></table>
</div>
<div>
  <div class="section-title" style="color:#4CAF50">▼ Soportes — Vol Buy</div>
  <table><tr><th>Rank</th><th>Precio</th><th>Vol Buy</th><th>Buy%</th><th>Sell%</th><th>Seg Dom</th><th>Score</th></tr><tbody id="ram-sv"></tbody></table>
  <div class="section-title" style="color:#4CAF50;margin-top:8px">▼ Soportes — Trades</div>
  <table><tr><th>Rank</th><th>Precio</th><th>Trades</th><th>Buy%</th><th>Sell%</th><th>Seg Dom</th></tr><tbody id="ram-st"></tbody></table>
</div>
</div>

<!-- CVD + Divergencia -->
<div class="grid" style="margin-top:10px">
<div>
  <div class="section-title">CVD por Ventana</div>
  <table><tr><th>Ventana</th><th>Buy</th><th>Sell</th><th>CVD</th><th>Dom</th></tr><tbody id="ram-cvd"></tbody></table>
</div>
<div>
  <div class="section-title">Divergencia Smart vs Retail</div>
  <table><tr><th>Ventana</th><th>Retail Buy%</th><th>Smart Buy%</th><th>Señal</th><th>Intensidad</th></tr><tbody id="ram-div"></tbody></table>
</div>
</div>

<!-- Indicadores por grupo -->
<div class="section-title" style="margin-top:10px">Indicadores por Grupo de Segmentos</div>
<table>
<tr><th>Grupo</th><th>RSI</th><th>MACD</th><th>Signal</th><th>Hist</th><th>VWAP</th><th>P.Buy</th><th>P.Sell</th><th>Buy%</th><th>Vol</th></tr>
<tbody id="ram-ind"></tbody>
</table>

<!-- VWAP sesiones -->
<div class="section-title" style="margin-top:10px">VWAP por Sesión</div>
<table>
<tr><th>Sesión</th><th>VWAP</th><th>Precio vs VWAP</th><th>Buy%</th><th>Vol Total</th><th>Trades</th></tr>
<tbody id="ram-vwap"></tbody>
</table>
</div>
</div>
</div>
<div class="modal-bg" id="modal"><div class="modal"><h2 id="modal-title">Confirmar</h2><p id="modal-msg"></p><p class="highlight" id="modal-time"></p><p class="muted" id="modal-detail"></p><div class="modal-btns"><button class="btn-confirm" onclick="modalConfirm()">Confirmar</button><button class="btn-cancel" onclick="modalCancel()">Cancelar</button></div></div></div>
<script>
let D=null,si=0,fi=0,pendingPar='',paresInit=false;
function showTab(id,el){document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));document.getElementById(id).classList.add('active');el.classList.add('active');if(id==='indicadores')srCargarArchivos();if(id==='parquet')pqRefresh()}
function stc(){si=+document.getElementById('ss-tf').value;fi=+document.getElementById('fs-tf').value;if(D)rS(D)}
function fN(n){if(n>=1e9)return(n/1e9).toFixed(2)+'B';if(n>=1e6)return(n/1e6).toFixed(2)+'M';if(n>=1e3)return(n/1e3).toFixed(2)+'K';return n.toFixed(2)}
function fI(n){return n.toString().replace(/\B(?=(\d{3})+(?!\d))/g,",")}
function fP(n){return n.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}
function clsDestello(buy,sell){return buy>=60?'destello-verde':sell>=60?'destello-rojo':'destello-ambar'}
function wrapBuy(pct,rank){
    // Envuelve el % de buy existente con badge si está en top3
    const cls=['1','2','3'][rank]||'3';
    if(pct>=52) return `<span class="badge-dom badge-g${cls}">${pct.toFixed(1)}%</span>`;
    return `<span class="buy">${pct.toFixed(1)}%</span>`;
}
function wrapSell(pct,rank){
    const cls=['1','2','3'][rank]||'3';
    if(pct>=52) return `<span class="badge-dom badge-r${cls}">${pct.toFixed(1)}%</span>`;
    return `<span class="sell">${pct.toFixed(1)}%</span>`;
}
function rT(d,id){
    const sorted=[...d].sort((a,b)=>b.volumen-a.volumen);
    const top3=sorted.slice(0,3).map(t=>t.nombre);
    document.getElementById(id).innerHTML=d.map(t=>{
        const rank=top3.indexOf(t.nombre);
        const buyCell=rank>=0?wrapBuy(t.buy_pct,rank):`<span class="buy">${t.buy_pct.toFixed(1)}%</span>`;
        const sellCell=rank>=0?wrapSell(t.sell_pct,rank):`<span class="sell">${t.sell_pct.toFixed(1)}%</span>`;
        return`<tr><td><b>${t.nombre}</b></td><td style="color:${t.fill_pct>=100?'#C9A84C':'#666'}">${t.fill_pct.toFixed(0)}%</td><td>${fI(t.trades)}</td><td>${fN(t.volumen)}</td><td>${buyCell}</td><td>${sellCell}</td></tr>`;
    }).join('')
}
function rSg(d,id){
    const sorted=[...d].sort((a,b)=>b.pct_vol-a.pct_vol);
    const top3=sorted.slice(0,3).map(s=>s.nombre);
    document.getElementById(id).innerHTML=d.map(s=>{
        const rank=top3.indexOf(s.nombre);
        const buyCell=rank>=0?wrapBuy(s.buy_pct,rank):`<span class="buy">${s.buy_pct.toFixed(1)}%</span>`;
        const sellCell=rank>=0?wrapSell(s.sell_pct,rank):`<span class="sell">${s.sell_pct.toFixed(1)}%</span>`;
        return`<tr><td><b>${s.nombre}</b></td><td>${fI(s.trades)}</td><td>${fN(s.volumen)}</td><td>${s.pct_vol.toFixed(1)}%</td><td>${buyCell}</td><td>${sellCell}</td></tr>`;
    }).join('')
}
function rS(d){if(d.spot.segmentos[si])rSg(d.spot.segmentos[si],'ss');if(d.futures.segmentos[fi])rSg(d.futures.segmentos[fi],'fs')}
function onParChange(){const sel=document.getElementById('par-sel');const nuevo=sel.value;if(D&&nuevo===D.par)return;pendingPar=nuevo;document.getElementById('modal-title').textContent='Cambiar par';document.getElementById('modal-msg').innerHTML='¿Cambiar a <b style="color:#E6C35C">'+nuevo+'</b>?';document.getElementById('modal-time').textContent='';document.getElementById('modal-detail').textContent='Volcado y cambio en el próximo ciclo de 8hrs';document.getElementById('modal').classList.add('show');if(D)sel.value=D.par}
function onStop(){pendingPar='__STOP__';document.getElementById('modal-title').textContent='Stop programado';document.getElementById('modal-msg').innerHTML='¿Detener el sistema?';document.getElementById('modal-time').textContent='';document.getElementById('modal-detail').textContent='Volcado y stop en el próximo ciclo de 8hrs';document.getElementById('modal').classList.add('show')}
async function modalConfirm(){document.getElementById('modal').classList.remove('show');if(pendingPar==='__STOP__'){await fetch('/api/stop',{method:'POST',headers:{'Content-Type':'application/json'}})}else{await fetch('/api/cambiar-par',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({nuevo_par:pendingPar})})}}
function modalCancel(){document.getElementById('modal').classList.remove('show')}
function rOB(d){
    document.getElementById('ob-bid-total').textContent=fN(d.ob.bid_total);
    document.getElementById('ob-ask-total').textContent=fN(d.ob.ask_total);
    document.getElementById('ob-spread').textContent='$'+d.ob.spread.toFixed(2)+' ('+d.ob.spread_pct.toFixed(4)+'%)';
    document.getElementById('ob-mid').textContent='$'+fP(d.ob.mid_price);
    document.getElementById('ob-bar-bid').style.width=d.ob.bid_pct.toFixed(0)+'%';
    document.getElementById('ob-bar-bid').textContent='Bids '+d.ob.bid_pct.toFixed(1)+'%';
    document.getElementById('ob-bar-ask').style.width=d.ob.ask_pct.toFixed(0)+'%';
    document.getElementById('ob-bar-ask').textContent='Asks '+d.ob.ask_pct.toFixed(1)+'%';
    document.getElementById('ob-precio-actual').textContent='--- precio actual: $'+fP(d.ob.mid_price)+' ---';
    // Tabla profundidad + CVD (sin barras visuales, solo números)
    const pares=d.ob.pares_cvd||[];
    document.getElementById('ob-depth-body').innerHTML=[...pares].reverse().map(p=>{
        const cvdAbs=Math.abs(p.cvd);
        const cvdSign=p.cvd>=0?'+':'-';
        const cvdCls=p.cvd>500000?'cvd-pos':p.cvd<-500000?'cvd-neg':'cvd-neu';
        return '<tr>'
            +'<td class="td-ask">'+fN(p.ask_usdt)+'</td>'
            +'<td class="td-ap">$'+p.ask_precio.toFixed(0)+'</td>'
            +'<td class="td-cvd '+cvdCls+'">'+cvdSign+fN(cvdAbs)+'</td>'
            +'<td class="td-bp">$'+p.bid_precio.toFixed(0)+'</td>'
            +'<td class="td-bid">'+fN(p.bid_usdt)+'</td>'
            +'</tr>';
    }).join('');
    // Totales pie
    const cvdG=d.ob.cvd_global;
    const cvdGCls=cvdG>500000?'#4CAF50':cvdG<-500000?'#CF6679':'#888';
    document.getElementById('ob-ask-total-pie').textContent=fN(d.ob.ask_total);
    document.getElementById('ob-bid-total-pie').textContent=fN(d.ob.bid_total);
    document.getElementById('ob-cvd-global').innerHTML='<span style="color:'+cvdGCls+'">CVD '+(cvdG>=0?'+':'')+fN(cvdG)+'</span>';
    document.getElementById('ob-cvd-badge').innerHTML='<span style="color:'+cvdGCls+'">CVD '+(cvdG>=0?'+':'')+fN(cvdG)+'</span>';
    // Liq — actualizar stats globales para el badge de la pestaña
    document.getElementById('liq-total').textContent=fI(d.liqs.total_count);
    document.getElementById('liq-long').textContent='$'+fN(d.liqs.total_long);
    document.getElementById('liq-short').textContent='$'+fN(d.liqs.total_short);
    rLiq(d.liqs);
}
async function upd(){try{const r=await fetch('/api/dashboard');const d=await r.json();D=d;
if(!paresInit&&d.pares_favoritos){document.getElementById('par-sel').innerHTML=d.pares_favoritos.map(p=>`<option value="${p}" ${p===d.par?'selected':''}>${p}</option>`).join('');paresInit=true}
document.getElementById('precio').textContent=d.precio>0?'$'+fP(d.precio):'--';
    document.getElementById('precio-fut').textContent=d.precio_futures>0?'$'+fP(d.precio_futures):'--';
    // Countdown volcado
    if(d.secs_volcado>0){
        const h=Math.floor(d.secs_volcado/3600);
        const m=Math.floor((d.secs_volcado%3600)/60);
        const s=d.secs_volcado%60;
        document.getElementById('countdown-volcado').textContent=
            'volcado '+String(h).padStart(2,'0')+':'+String(m).padStart(2,'0')+':'+String(s).padStart(2,'0');
    }
document.getElementById('status').textContent=d.activo?'ACTIVO':'DETENIDO';document.getElementById('status').className=d.activo?'status on':'status off';
document.getElementById('ts').textContent=fI(d.spot.trades_total);document.getElementById('tf').textContent=fI(d.futures.trades_total);document.getElementById('tt').textContent=fI(d.spot.trades_total+d.futures.trades_total);
rT(d.spot.temporalidades,'st');rT(d.futures.temporalidades,'ft');rS(d);rOB(d);
if(d.sesion_ant.valido){document.getElementById('si').innerHTML='<span style="color:#B0B0B0">Anterior '+d.sesion_ant.nombre+': O$'+fP(d.sesion_ant.precio_apertura)+' PD:<span style="color:'+(d.sesion_ant.pd_dir==='Buy'?'#E6C35C':'#B0B0B0')+'">'+d.sesion_ant.pd_dir+'</span> '+d.sesion_ant.pd+' | C$'+fP(d.sesion_ant.precio_cierre)+'</span>'}else{document.getElementById('si').innerHTML='<span style="color:#666">Anterior: sin datos</span>'}
if(d.sesion_act.valido){document.getElementById('sp').innerHTML='<span style="color:#E6C35C">'+d.sesion_act.nombre+': O$'+fP(d.sesion_act.precio_apertura)+' PD:<span style="color:'+(d.sesion_act.pd_dir==='Buy'?'#E6C35C':'#B0B0B0')+'">'+d.sesion_act.pd_dir+'</span> '+d.sesion_act.pd+' | $'+fP(d.sesion_act.precio_cierre)+'</span>'}else{document.getElementById('sp').innerHTML='<span style="color:#666">Actual: esperando...</span>'}
document.getElementById('utc').textContent='UTC: '+new Date().toISOString().substr(11,8);
const cb=document.getElementById('cambio-banner');if(d.cambio_pendiente.activo){cb.classList.add('show');cb.textContent=d.cambio_pendiente.es_stop?'⏳ Stop programado - Volcado a las '+d.cambio_pendiente.hora_volcado:'⏳ Cambio a '+d.cambio_pendiente.nuevo_par+' - Volcado a las '+d.cambio_pendiente.hora_volcado}else{cb.classList.remove('show')}
const pqNext=document.getElementById('pq-next');if(pqNext)pqNext.textContent=d.cambio_pendiente.hora_volcado||'--';
}catch(e){document.getElementById('status').textContent='DESCONECTADO';document.getElementById('status').className='status off'}}
// ── Parquet ──
async function pqRefresh(){
    const par=document.getElementById('par-sel')?.value||'SOLUSDT';
    try{const r=await fetch('/api/parquet/info?par='+par);const d=await r.json();
    document.getElementById('pq-spot-count').textContent=d.spot.length;
    document.getElementById('pq-fut-count').textContent=d.futures.length;
    const pqRow=(f,mkt)=>`<tr><td style="font-size:11px">${f.nombre}</td><td style="color:#C9A84C">${f.size_kb}KB</td><td><button onclick="pqEliminar('${f.nombre}','${mkt}')" style="background:#4a2d2d;color:#CF6679;border:1px solid #CF667966;padding:1px 6px;font-size:10px;cursor:pointer;font-family:'Courier New',monospace;border-radius:2px">✕</button></td></tr>`;
    document.getElementById('pq-spot-list').innerHTML=d.spot.length?d.spot.map(f=>pqRow(f,'spot')).join(''):'<tr><td colspan="3" class="muted">Sin archivos aún</td></tr>';
    document.getElementById('pq-fut-list').innerHTML=d.futures.length?d.futures.map(f=>pqRow(f,'futures')).join(''):'<tr><td colspan="3" class="muted">Sin archivos aún</td></tr>';
    }catch(e){console.error('Parquet info:',e)}}
pqRefresh();
async function pqEliminar(nombre,mercado){
    if(!confirm('¿Eliminar '+nombre+'?'))return;
    try{
        const r=await fetch('/api/parquet/eliminar',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({nombre,mercado})});
        const d=await r.json();
        if(d.ok){pqRefresh();}else{alert('Error: '+d.error);}
    }catch(e){alert('Error: '+e.message);}
}
// ── Indicadores S/R — análisis manual ──
let srResultadoActual=null;
async function srCargarArchivos(){
    const par=document.getElementById('par-sel')?.value||'SOLUSDT';
    const mercado=document.getElementById('sr-mercado').value;
    const sel=document.getElementById('sr-archivo');
    sel.innerHTML='<option value="">— cargando... —</option>';
    try{
        const r=await fetch(`/api/sr/archivos?par=${par}&mercado=${mercado}`);
        const d=await r.json();
        sel.innerHTML='<option value="">— Seleccionar Parquet —</option>';
        (d.archivos||[]).forEach(a=>{const o=document.createElement('option');o.value=a.ruta;o.textContent=a.mercado.toUpperCase()+' · '+a.nombre+' ('+a.tamano_kb+'KB)';sel.appendChild(o)});
        if(sel.options.length>1)sel.selectedIndex=1;
    }catch(e){sel.innerHTML='<option value="">— Error cargando —</option>'}
}
async function srAnalizar(){
    const par=document.getElementById('par-sel')?.value||'SOLUSDT';
    const ruta=document.getElementById('sr-archivo').value;
    if(!ruta){srStatus('⚠ Selecciona un archivo','#D4A017');return}
    const btn=document.getElementById('btn-sr-analizar');
    btn.disabled=true;srStatus('Analizando...','#666');
    document.getElementById('btn-sr-exportar').style.display='none';
    srResultadoActual=null;
    try{
        const r=await fetch('/api/sr/analizar',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({par,ruta})});
        const d=await r.json();
        if(!d.ok||d.resultado?.error){srStatus('✗ '+(d.resultado?.error||d.error||'Error'),'#CF6679')}
        else{srResultadoActual=d.resultado;srRenderizar(d.resultado);srStatus('✓ Análisis completado','#4CAF50');document.getElementById('btn-sr-exportar').style.display=''}
    }catch(e){srStatus('✗ '+e.message,'#CF6679')}
    finally{btn.disabled=false}
}
async function srExportar(){
    if(!srResultadoActual)return;
    const btn=document.getElementById('btn-sr-exportar');btn.disabled=true;srStatus('Guardando...','#666');
    try{
        const r=await fetch('/api/sr/exportar',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({resultado:srResultadoActual})});
        const d=await r.json();srStatus(d.ok?'✓ Guardado':'✗ '+d.error,d.ok?'#4CAF50':'#CF6679');
    }catch(e){srStatus('✗ '+e.message,'#CF6679')}
    finally{btn.disabled=false}
}
function srRenderizar(r){
    const fmt=n=>'$'+n.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
    const ts=new Date(r.timestamp);
    document.getElementById('sr-ts').textContent=ts.toISOString().replace('T',' ').substring(0,19)+' UTC';
    document.getElementById('sr-sesion').textContent=r.sesion||'—';
    document.getElementById('sr-precio').textContent=fmt(r.precio_ref);
    document.getElementById('sr-bucket').textContent='±'+fmt(r.bucket_size)+' (0.6%)';
    const rCls=i=>i===0?'buy':i===1?'muted muted':'muted';
    const fillV=(id,ns)=>{document.getElementById(id).innerHTML=ns.map((n,i)=>'<tr><td class="'+rCls(i)+'"><b>#'+n.rank+'</b></td><td>'+fmt(n.precio_nivel)+'</td><td>'+fN(n.volumen_usdt)+'</td><td class="buy">'+n.buy_pct.toFixed(1)+'%</td><td class="sell">'+n.sell_pct.toFixed(1)+'%</td><td><b>P'+n.seg_dominante+'</b></td><td style="color:#D4A017">'+fN(n.score)+'</td></tr>').join('')||'<tr><td colspan="7" class="muted">Sin datos</td></tr>'};
    const fillT=(id,ns)=>{document.getElementById(id).innerHTML=ns.map((n,i)=>'<tr><td class="'+rCls(i)+'"><b>#'+n.rank+'</b></td><td>'+fmt(n.precio_nivel)+'</td><td>'+fI(n.trade_count)+'</td><td class="buy">'+n.buy_pct.toFixed(1)+'%</td><td class="sell">'+n.sell_pct.toFixed(1)+'%</td><td><b>P'+n.seg_dominante+'</b></td></tr>').join('')||'<tr><td colspan="6" class="muted">Sin datos</td></tr>'};
    fillV('sr-rv',r.resistencias_vol||[]);fillV('sr-sv',r.soportes_vol||[]);
    fillT('sr-rt',r.resistencias_trades||[]);fillT('sr-st',r.soportes_trades||[]);
    document.getElementById('sr-pending').style.display='none';
}
function srStatus(msg,color){const el=document.getElementById('sr-status');el.textContent=msg;el.style.color=color}
document.getElementById('sr-mercado')?.addEventListener('change',srCargarArchivos);
// ── Liquidaciones ──
let liqVentanaMin=1440;
function liqCambiarVentana(val){liqVentanaMin=parseInt(val)}
function rLiq(liqs){
    // Ventanas temporales
    document.getElementById('liq-ventanas').innerHTML=(liqs.ventanas||[]).map(v=>{
        const domCls=v.dominancia==='LONG'?'long-liq':v.dominancia==='SHORT'?'short-liq':'muted';
        const pctL=v.total_usdt>0?((v.long_usdt/v.total_usdt)*100).toFixed(0)+'%':'—';
        const pctS=v.total_usdt>0?((v.short_usdt/v.total_usdt)*100).toFixed(0)+'%':'—';
        return '<tr><td style="color:#C9A84C;font-weight:bold">'+v.label+'</td>'
            +'<td class="long-liq">$'+fN(v.long_usdt)+' <span style="color:#555;font-size:10px">'+pctL+'</span></td>'
            +'<td class="short-liq">$'+fN(v.short_usdt)+' <span style="color:#555;font-size:10px">'+pctS+'</span></td>'
            +'<td>$'+fN(v.total_usdt)+'</td>'
            +'<td class="'+domCls+'"><b>'+(v.dominancia==='EQ'?'—':v.dominancia)+'</b></td></tr>';
    }).join('');
    // CVD por escalón de precio
    const esc=(liqs.cvd_escalones||[]).filter(e=>{
        if(liqVentanaMin>=1440) return true;
        return true; // filtro por ventana se aplica en backend — aquí mostramos todo
    });
    const maxAbs=Math.max(...esc.map(e=>Math.abs(e.cvd)),1);
    document.getElementById('liq-cvd-escalones').innerHTML=esc.length===0
        ?'<p style="color:#444;padding:8px">Sin liquidaciones en el período</p>'
        :esc.map(e=>{
            const pct=Math.abs(e.cvd)/maxAbs;
            const barW=Math.round(pct*140);
            const isLong=e.cvd>0;
            const col=isLong?'#CF6679':'#4CAF50'; // long liq=rojo, short liq=verde
            const sign=isLong?'+':'-';
            const bar='<div style="display:inline-block;width:'+barW+'px;height:10px;background:'+col+';border-radius:2px;opacity:0.8;vertical-align:middle"></div>';
            return '<div style="display:flex;align-items:center;gap:8px;padding:2px 4px;border-bottom:1px solid #111">'
                +'<span style="color:#C9A84C;min-width:48px;text-align:right;font-weight:bold">$'+e.precio.toFixed(0)+'</span>'
                +'<span style="color:'+col+';min-width:80px;text-align:right">'+sign+fN(Math.abs(e.cvd))+'</span>'
                +bar
                +'<span style="color:#555;font-size:10px">'+fI(e.count)+' liq | total:$'+fN(e.total_usdt)+'</span>'
                +'</div>';
        }).join('');
    // Por segmento
    document.getElementById('liq-seg').innerHTML=liqs.por_segmento
        .filter(s=>s.long_count>0||s.short_count>0)
        .map(s=>'<tr><td><b>'+s.nombre+'</b></td><td class="long-liq">'+fI(s.long_count)+'</td><td class="long-liq">$'+fN(s.long_usdt)+'</td><td class="short-liq">'+fI(s.short_count)+'</td><td class="short-liq">$'+fN(s.short_usdt)+'</td></tr>').join('');
    // Recientes
    document.getElementById('liq-rec').innerHTML=liqs.recientes.map(l=>{
        const t=new Date(l.timestamp).toISOString().substr(11,8);
        const cls=l.side.includes('LONG')?'long-liq':'short-liq';
        return '<tr><td>'+t+'</td><td class="'+cls+'">'+l.side+'</td><td><b>'+l.segmento+'</b></td><td>$'+fN(l.usdt)+'</td><td>$'+fP(l.precio)+'</td></tr>';
    }).join('');
}

// ── SQL Lab ──
let sqlResult=null,sqlIdActivo='';
function fmtSqlVal(v,col){
    // Timestamp: número de 13 dígitos → fecha/hora
    if(typeof v==='string'&&/^\d{13}$/.test(v.trim())){
        const d=new Date(parseInt(v));
        return d.toISOString().replace('T',' ').substring(0,19);
    }
    // Número con muchos decimales → 2 decimales
    if(typeof v==='string'&&/^\d+\.\d{3,}$/.test(v.trim())){
        return parseFloat(v).toFixed(2);
    }
    return v;
}
let sqlArchivos=[];
let sqlModoArchivo='all';
function sqlSetArchivo(modo){
    sqlModoArchivo=modo;
    ['all','rango','uno'].forEach(m=>{
        const btn=document.getElementById('sql-modo-'+m);
        const panel=document.getElementById('sql-sel-'+m);
        if(btn){btn.style.background=m===modo?'#2C2418':'#111';btn.style.color=m===modo?'#C9A84C':'#666';btn.style.borderColor=m===modo?'#C9A84C':'#333';}
        if(panel){panel.style.display=m===modo?(m==='rango'?'flex':'block'):'none';}
    });
}
async function sqlCargarArchivos(){
    const par=document.getElementById('sql-par').value;
    const mkt=document.getElementById('sql-mkt').value;
    if(!par)return;
    try{
        const r=await fetch('/api/parquet/info?par='+par+'&mercado='+mkt);
        const d=await r.json();
        const lista=(mkt==='spot'?d.spot:d.futures).slice().reverse();
        sqlArchivos=lista.map(f=>f.nombre);
        // Poblar desde/hasta y individual
        const optsHtml=lista.map(f=>`<option value="${f.nombre}">${f.nombre} (${f.size_kb}KB)</option>`).join('');
        const sel1=document.getElementById('sql-desde');
        const sel2=document.getElementById('sql-hasta');
        const sel3=document.getElementById('sql-archivo-uno');
        if(sel1)sel1.innerHTML=optsHtml;
        if(sel2){sel2.innerHTML=optsHtml;if(lista.length>0)sel2.selectedIndex=lista.length-1;}
        if(sel3)sel3.innerHTML=optsHtml;
        document.getElementById('sql-sel-all').textContent='Escanea todos los archivos ('+lista.length+')';
    }catch(e){console.error('cargar archivos:',e)}
}
async function sqlInit(){
    try{
        const [ar,pr]=await Promise.all([fetch('/api/sql/queries'),fetch('/api/sql/pares')]);
        const analisis=await ar.json();const pares=await pr.json();
        document.getElementById('sql-predef').innerHTML=analisis.map(a=>`
            <button onclick="sqlEjecutar('${a.id}')"
                style="background:#222;border:1px solid #333;color:#D4D4D4;padding:8px 12px;
                text-align:left;cursor:pointer;font-family:'Courier New',monospace;font-size:11px;
                border-radius:3px;transition:border-color 0.2s"
                onmouseover="this.style.borderColor='#C9A84C'"
                onmouseout="this.style.borderColor='#333'"
                title="${a.descripcion}">
                <b style="color:#C9A84C">${a.nombre}</b><br>
                <span style="color:#666;font-size:10px">${a.descripcion}</span>
            </button>`).join('');
        const sel=document.getElementById('sql-par');
        sel.innerHTML=pares.length>0?pares.map(p=>`<option value="${p}">${p}</option>`).join(''):'<option value="SOLUSDT">SOLUSDT</option>';
        sqlCargarArchivos();
    }catch(e){console.error('SQL init:',e)}
}
async function sqlEjecutar(id){
    const par=document.getElementById('sql-par').value;
    const mkt=document.getElementById('sql-mkt').value;
    sqlIdActivo=id;
    document.getElementById('sql-info').textContent='Ejecutando Polars Lazy...';
    document.getElementById('sql-tiempo').textContent='';
    document.getElementById('sql-thead').innerHTML='';
    document.getElementById('sql-tbody').innerHTML='<tr><td style="color:#C9A84C;padding:8px">⏳ Procesando...</td></tr>';
    try{
        // Calcular archivo(s) según modo seleccionado
        let archivo='all';
        if(sqlModoArchivo==='uno'){
            archivo=document.getElementById('sql-archivo-uno')?.value||'all';
        } else if(sqlModoArchivo==='rango'){
            const desde=document.getElementById('sql-desde')?.value||'';
            const hasta=document.getElementById('sql-hasta')?.value||'';
            // Filtrar archivos dentro del rango lexicográfico
            const enRango=sqlArchivos.filter(f=>f>=desde&&f<=hasta);
            archivo=enRango.length===1?enRango[0]:(enRango.length>0?'rango:'+enRango.join(','):'all');
        }
        const r=await fetch('/api/sql/ejecutar',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id,par,mercado:mkt,archivo})});
        const d=await r.json();sqlResult=d;
        if(d.error){document.getElementById('sql-info').textContent='Error';document.getElementById('sql-thead').innerHTML='';document.getElementById('sql-tbody').innerHTML=`<tr><td style="color:#CF6679;padding:8px">${d.error}</td></tr>`;return;}
        document.getElementById('sql-tiempo').textContent=`(${d.tiempo_ms}ms | ${d.total_filas} filas)`;
        document.getElementById('sql-info').textContent=`${d.total_filas} filas`;
        document.getElementById('sql-thead').innerHTML='<tr>'+d.columnas.map(c=>`<th>${c}</th>`).join('')+'</tr>';
        document.getElementById('sql-tbody').innerHTML=d.filas.slice(0,500).map(f=>'<tr>'+f.map((v,i)=>`<td>${fmtSqlVal(v,d.columnas[i])}</td>`).join('')+'</tr>').join('');
    }catch(e){document.getElementById('sql-info').textContent='Error: '+e}
}
function sqlExportar(){
    if(!sqlResult||!sqlResult.columnas)return;
    let csv=sqlResult.columnas.join(',')+'\\n';
    csv+=sqlResult.filas.map(f=>f.map(v=>String(v).includes(',')?`"${v}"`:v).join(',')).join('\\n');
    const a=document.createElement('a');a.href='data:text/csv;charset=utf-8,'+encodeURIComponent(csv);
    a.download=`forja_${sqlIdActivo}.csv`;a.click();
}
sqlInit();
// ── RAM Lab ──
let ramData=null,ramVentanaIdx=0;
function sqlSetModo(modo){
    const isRam=modo==='ram';
    document.getElementById('sql-parquet-mode').style.display=isRam?'none':'block';
    document.getElementById('sql-ram-mode').style.display=isRam?'block':'none';
    document.getElementById('btn-modo-parquet').style.background=isRam?'#111':'#2C2418';
    document.getElementById('btn-modo-parquet').style.color=isRam?'#666':'#C9A84C';
    document.getElementById('btn-modo-ram').style.background=isRam?'#2C2418':'#111';
    document.getElementById('btn-modo-ram').style.color=isRam?'#C9A84C':'#666';
}
async function ramAnalizar(){
    const mkt=document.getElementById('ram-mkt').value;
    const horas=parseInt(document.getElementById('ram-horas-ind').value);
    document.getElementById('ram-info').textContent='Analizando RAM...';
    try{
        const r=await fetch('/api/ram/analizar',{
            method:'POST',headers:{'Content-Type':'application/json'},
            body:JSON.stringify({mercado:mkt,horas_indicadores:horas})
        });
        const d=await r.json();ramData=d;
        document.getElementById('ram-info').textContent=
            d.total_trades_ram.toLocaleString()+' trades | ref: $'+fP(d.precio_ref);
        ramRenderSR(0);
        ramRenderCVD(d);
        ramRenderDiv(d);
        ramRenderInd(d);
        ramRenderVwap(d);
        // Tabs de ventanas
        const tabs=document.getElementById('ram-sr-tabs');
        tabs.innerHTML=d.sr_ventanas.map((v,i)=>
            `<button onclick="ramRenderSR(${i})" style="background:${i===0?'#2C2418':'#111'};color:${i===0?'#C9A84C':'#666'};border:1px solid ${i===0?'#C9A84C':'#333'};padding:4px 10px;font-family:'Courier New',monospace;font-size:11px;cursor:pointer;border-radius:3px" id="ram-tab-${i}">${v.horas}h</button>`
        ).join('');
    }catch(e){document.getElementById('ram-info').textContent='Error: '+e.message}
}
function ramRenderSR(idx){
    if(!ramData)return;
    ramVentanaIdx=idx;
    const v=ramData.sr_ventanas[idx];
    if(!v)return;
    // Actualizar tab activo
    ramData.sr_ventanas.forEach((_,i)=>{
        const btn=document.getElementById('ram-tab-'+i);
        if(btn){btn.style.background=i===idx?'#2C2418':'#111';btn.style.color=i===idx?'#C9A84C':'#666';btn.style.borderColor=i===idx?'#C9A84C':'#333'}
    });
    const fmt=n=>'$'+fP(n);
    const fillV=(id,ns)=>{document.getElementById(id).innerHTML=ns.map((n,i)=>
        '<tr><td class="'+(i===0?'buy':'muted')+'"><b>#'+n.rank+'</b></td>'
        +'<td>'+fmt(n.precio)+'</td><td>'+fN(n.vol_usdt)+'</td>'
        +'<td class="buy">'+n.buy_pct.toFixed(1)+'%</td>'
        +'<td class="sell">'+n.sell_pct.toFixed(1)+'%</td>'
        +'<td><b>P'+n.seg_dominante+'</b></td>'
        +'<td style="color:#D4A017">'+fN(n.score)+'</td></tr>'
    ).join('')||'<tr><td colspan="7" class="muted">Sin datos</td></tr>'};
    const fillT=(id,ns)=>{document.getElementById(id).innerHTML=ns.map((n,i)=>
        '<tr><td class="'+(i===0?'buy':'muted')+'"><b>#'+n.rank+'</b></td>'
        +'<td>'+fmt(n.precio)+'</td><td>'+fI(n.trades)+'</td>'
        +'<td class="buy">'+n.buy_pct.toFixed(1)+'%</td>'
        +'<td class="sell">'+n.sell_pct.toFixed(1)+'%</td>'
        +'<td><b>P'+n.seg_dominante+'</b></td></tr>'
    ).join('')||'<tr><td colspan="6" class="muted">Sin datos</td></tr>'};
    fillV('ram-rv',v.resistencias_vol);fillV('ram-sv',v.soportes_vol);
    fillT('ram-rt',v.resistencias_trades);fillT('ram-st',v.soportes_trades);
}
function ramRenderCVD(d){
    document.getElementById('ram-cvd').innerHTML=(d.cvd_ventanas||[]).map(v=>{
        const cvdCls=v.cvd>0?'buy':'sell';
        return '<tr><td style="color:#C9A84C;font-weight:bold">'+v.horas+'h</td>'
            +'<td class="buy">'+fN(v.vol_buy)+'</td>'
            +'<td class="sell">'+fN(v.vol_sell)+'</td>'
            +'<td class="'+cvdCls+'" style="font-weight:bold">'+(v.cvd>=0?'+':'')+fN(v.cvd)+'</td>'
            +'<td class="'+cvdCls+'">'+v.dominancia+'</td></tr>';
    }).join('');
}
function ramRenderDiv(d){
    document.getElementById('ram-div').innerHTML=(d.divergencia||[]).map(v=>{
        const cls=v.divergencia==='SMART_COMPRA'?'buy':v.divergencia==='SMART_VENDE'?'sell':'muted';
        return '<tr><td style="color:#C9A84C;font-weight:bold">'+v.horas+'h</td>'
            +'<td class="buy">'+v.retail_buy_pct.toFixed(1)+'%</td>'
            +'<td class="buy">'+v.smart_buy_pct.toFixed(1)+'%</td>'
            +'<td class="'+cls+'"><b>'+v.divergencia+'</b></td>'
            +'<td>'+v.intensidad.toFixed(1)+'%</td></tr>';
    }).join('');
}
function ramRenderInd(d){
    document.getElementById('ram-ind').innerHTML=(d.indicadores_grupos||[]).map(g=>{
        const rsiCls=g.rsi>60?'buy':g.rsi<40?'sell':'muted';
        const histCls=g.macd.histogram>0?'buy':'sell';
        return '<tr>'
            +'<td style="color:#C9A84C;font-weight:bold">'+g.grupo+'</td>'
            +'<td class="'+rsiCls+'">'+g.rsi.toFixed(1)+'</td>'
            +'<td>'+(g.macd.macd>=0?'+':'')+g.macd.macd.toFixed(4)+'</td>'
            +'<td>'+g.macd.signal.toFixed(4)+'</td>'
            +'<td class="'+histCls+'">'+(g.macd.histogram>=0?'+':'')+g.macd.histogram.toFixed(4)+'</td>'
            +'<td style="color:#D4A017">$'+fP(g.vwap)+'</td>'
            +'<td class="buy">$'+fP(g.precio_prom_buy)+'</td>'
            +'<td class="sell">$'+fP(g.precio_prom_sell)+'</td>'
            +'<td class="buy">'+g.buy_pct.toFixed(1)+'%</td>'
            +'<td>'+fN(g.vol_total)+'</td>'
            +'</tr>';
    }).join('');
}
function ramRenderVwap(d){
    document.getElementById('ram-vwap').innerHTML=(d.vwap_sesiones||[]).map(v=>{
        const diff=v.precio_actual_vs_vwap;
        const diffCls=diff>0?'buy':diff<0?'sell':'muted';
        return '<tr>'
            +'<td style="color:#C9A84C;font-weight:bold">'+v.nombre+'</td>'
            +'<td style="color:#D4A017">$'+fP(v.vwap)+'</td>'
            +'<td class="'+diffCls+'">'+(diff>=0?'+':'')+diff.toFixed(2)+'</td>'
            +'<td class="buy">'+v.buy_pct.toFixed(1)+'%</td>'
            +'<td>'+fN(v.vol_total)+'</td>'
            +'<td>'+fI(v.trades)+'</td>'
            +'</tr>';
    }).join('');
}

setInterval(upd,1000);upd();
</script></body></html>"##;
