// zeruel web: UI embebida (Fase 6, rediseño 2026-08).
//
// tiny_http (sincronico) + la UI en `ui.html`, empotrada con include_str!.
// Estilo: verde bosque (igual que docs/MANUAL.html), con tema oscuro.
// UI por tabs: Dashboard · Runs · Métricas · Alertas · Cola y Logs, mas una
// landing por DAG en `#/dag/<nombre>`.
//
// API:
//   GET  /                    -> UI
//   GET  /api/dags            -> [{dag, runs, ok, fail, ..., ultimo_estado,
//                                 racha_fallos, ultimas}]
//   GET  /api/graph?dag=X     -> {name, nodes, edges}
//   GET  /api/runs            -> {filas, total, offset, limite, dags}
//        filtros: ?q=texto | ?dag=X | ?estado=Failed | ?desde=&hasta=
//                 ?dur_min=<seg> | ?ram_min=<MB> | ?reintentos=1
//        orden:   ?orden=<col>&dir=asc|desc   paginado: ?limite=&offset=
//   GET  /api/run/<id>        -> run + tasks (por id, contra la DB)
//   GET  /api/queue           -> {items:[...], max_size}
//   GET  /api/hygiene         -> pulso
//   GET  /api/salud           -> consumo de zeruel: RAM por proceso, disco, trabajo
//   GET  /api/metrics         -> {volumen, dags, tareas, actividad, sla, ...}
//        filtros: ?dias=N | ?desde=<unix>&hasta=<unix> | ?dag=X
//                 ?ram_min=<MB> | ?dur_min=<seg> | ?fallos=1
//        El default es HOY, no todo: abrir la pestaña tiene que ser barato.
//   GET  /api/alerts          -> {sla_violaciones, fails_24h, db_warning, ...}
//   GET  /api/logs            -> ultimas lineas del log mas nuevo
//   POST /api/retry           -> encola de nuevo el ultimo run fallido
//   POST /api/run             -> encola un run nuevo {dag, param, priority}
//   POST /api/run/<id>/cancel -> pide cancelar (queda en la DB: cruza procesos)
//
// La UI escapa TODO lo que viene del store antes de meterlo en el DOM: `param`
// es texto libre que entra por POST /api/run (era un XSS almacenado, N-32).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use serde_json::json;
use tiny_http::{Header, Response, Server};
use zeruel_core::Store;

/// Arranca el servidor web. Con `max > 0` sale despues de N requests (tests).
///
/// `bind` es `127.0.0.1` por defecto (N-31): esta UI no tiene autenticacion y
/// tiene endpoints POST que encolan y cancelan runs, asi que abrirla a toda la
/// red tiene que ser una decision explicita (`--bind 0.0.0.0`).
pub fn run(
    state: PathBuf,
    pipelines: PathBuf,
    port: u16,
    max: u32,
    bind: &str,
    token: Option<String>,
    config: Option<PathBuf>,
) -> Result<()> {
    run_con_bandera(state, pipelines, port, max, bind, token, config, None)
}

/// Igual, pero con una bandera de apagado que puede encender otro hilo.
///
/// Hace falta para correr la web como servicio: la orden de parada llega por
/// una llamada del sistema, no por una señal, y el bucle tiene que poder
/// enterarse. Sin esto quedaria bloqueado esperando la proxima request y el
/// supervisor terminaria matandolo.
#[allow(clippy::too_many_arguments)]
pub fn run_con_bandera(
    state: PathBuf,
    pipelines: PathBuf,
    port: u16,
    max: u32,
    bind: &str,
    token: Option<String>,
    config: Option<PathBuf>,
    apagando: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
) -> Result<()> {
    // Se fija una sola vez, al arrancar: `pipelines_desplegados` la lee de aca
    // en vez de resolverla contra el directorio actual del proceso.
    let _ = DIR_PIPELINES.set(pipelines);
    // Igual con el archivo de configuracion: la pestaña Configuracion compara
    // lo que dice el archivo contra lo que el scheduler realmente esta usando,
    // y necesita saber de que archivo hablamos. Puede no haber ninguno, y eso
    // tambien es informacion: significa valores de fabrica.
    if let Some(c) = config {
        let _ = RUTA_CONFIG.set(c);
    }
    let store = Arc::new(Store::open(&state).context("abrir store")?);
    let addr = format!("{bind}:{port}");
    let server = Server::http(&addr).map_err(|e| anyhow::anyhow!("bind {addr}: {e}"))?;
    println!(
        "zeruel web: http://localhost:{port}  (state: {})",
        state.display()
    );
    if bind != "127.0.0.1" && bind != "localhost" {
        println!(
            "  [!] escuchando en {bind}: la UI queda expuesta a la red SIN autenticacion.\n      \
             Cualquiera que llegue al puerto puede encolar y cancelar runs."
        );
    }

    let mut n: u32 = 0;
    loop {
        // Con timeout en vez de bloquear indefinidamente: es lo que deja mirar
        // la bandera de apagado entre request y request. Medio segundo es
        // imperceptible al atender y suficiente para parar rapido.
        if apagando
            .as_ref()
            .is_some_and(|b| b.load(std::sync::atomic::Ordering::SeqCst))
        {
            println!("zeruel web: apagando");
            break;
        }
        let mut request = match server.recv_timeout(std::time::Duration::from_millis(500)) {
            Ok(Some(r)) => r,
            Ok(None) => continue, // no llego nada: se vuelve a mirar la bandera
            Err(e) => {
                tracing::error!("no pude recibir la request: {e}");
                continue;
            }
        };
        n += 1;
        let url = request.url().to_string();
        let method = request.method().as_str().to_string();
        let body_req = read_body(&mut request);
        let autorizado = token
            .as_deref()
            .is_none_or(|t| presenta_el_token(&request, t));
        let (status, body, ctype) = if autorizado {
            handle(&store, &method, &url, &body_req)
        } else {
            (
                401,
                r#"{"error":"falta el token. Mandalo como `Authorization: Bearer <token>` o `?token=<token>`"}"#
                    .to_string(),
                "application/json",
            )
        };
        let mut resp = Response::from_string(body).with_status_code(status);
        let h = Header::from_bytes(&b"Content-Type"[..], ctype.as_bytes()).unwrap();
        resp.add_header(h);
        let _ = request.respond(resp);
        if max > 0 && n >= max {
            break;
        }
    }
    Ok(())
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// ¿La request trae el token correcto?
///
/// Se acepta en el header `Authorization: Bearer <token>` (lo normal para una
/// API) o como `?token=<token>` en la query, que es lo unico que sirve al abrir
/// la UI en el navegador desde otra maquina.
///
/// La comparacion recorre siempre los dos valores enteros: comparar con `==` y
/// cortar en la primera diferencia filtra, por el tiempo que tarda, cuantos
/// caracteres del principio son correctos.
fn presenta_el_token(request: &tiny_http::Request, esperado: &str) -> bool {
    let del_header = request
        .headers()
        .iter()
        .find(|h| h.field.equiv("Authorization"))
        .and_then(|h| {
            let v = h.value.as_str();
            v.strip_prefix("Bearer ").map(|t| t.trim().to_string())
        });

    let del_query = split(request.url())
        .1
        .split('&')
        .find_map(|par| par.split_once('=').filter(|(k, _)| *k == "token"))
        .map(|(_, v)| url_decode(v));

    match del_header.or(del_query) {
        Some(t) => iguales_en_tiempo_constante(t.as_bytes(), esperado.as_bytes()),
        None => false,
    }
}

/// Compara sin cortar en la primera diferencia.
fn iguales_en_tiempo_constante(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut dif = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        dif |= x ^ y;
    }
    dif == 0
}

/// Despacha la URL. Devuelve (status, body, content-type).
fn handle(
    store: &Store,
    method: &str,
    path_query: &str,
    body: &str,
) -> (u16, String, &'static str) {
    let json = |v: &serde_json::Value| (200, v.to_string(), "application/json");
    let (path, query) = split(path_query);
    let method = method.to_uppercase();
    if method == "POST" {
        // Pausar y activar. Van por POST porque cambian estado: un GET que
        // modifica algo se dispara solo desde cualquier precarga del navegador.
        if let Some(dag) = path.strip_prefix("/api/dag/") {
            if let Some(n) = dag.strip_suffix("/pausar") {
                return json(&pausar(store, &url_decode(n), true));
            }
            if let Some(n) = dag.strip_suffix("/activar") {
                return json(&pausar(store, &url_decode(n), false));
            }
        }
        // Cerrar una alerta. La clave es el id de la corrida, `cola:<id>` o
        // `db`: ver Store::alerta_vista para por que no se borra nada.
        if let Some(clave) = path.strip_prefix("/api/alerta/") {
            if let Some(k) = clave.strip_suffix("/cerrar") {
                return json(&cerrar_alerta(store, &url_decode(k)));
            }
        }
        if path == "/api/alertas/cerrar-todas" {
            return json(&cerrar_todas(store));
        }
        // Sacar un item de la cola. Va por id de cola y no por UUID de run
        // porque un item que todavia no arranco NO TIENE run: desde la cola es
        // lo unico que se puede cancelar, y era justo lo que no se podia.
        if let Some(rest) = path.strip_prefix("/api/cola/") {
            if let Some(id_str) = rest.strip_suffix("/cancelar") {
                return json(&cancelar_cola(store, id_str));
            }
        }
        if path == "/api/retry" {
            return json(&retry(store));
        }
        if path == "/api/run" {
            return json(&enqueue(store, body));
        }
        if let Some(rest) = path.strip_prefix("/api/run/") {
            if let Some(id_str) = rest.strip_suffix("/cancel") {
                let id = uuid::Uuid::parse_str(id_str);
                return match id {
                    Ok(uid) => json(&cancel_run(store, uid)),
                    Err(_) => json(&json!({"error": "id invalido"})),
                };
            }
        }
    }
    match &*path {
        "/" => (200, UI_HTML.to_string(), "text/html; charset=utf-8"),
        "/api/dags" => json(&dags(store)),
        "/api/graph" => json(&graph(query_param(&query, "dag").as_deref())),
        "/api/runs" => json(&runs(store, &query)),
        "/api/queue" => json(&queue(store)),
        "/api/hygiene" => json(&hygiene(store)),
        "/api/salud" => json(&crate::salud::reporte(store)),
        "/api/config" => json(&configuracion(store)),
        "/api/codigo" => json(&codigo(query_param(&query, "dag").as_deref())),
        _ if path.starts_with("/api/run/") && path.ends_with("/log") => {
            let id = path
                .trim_start_matches("/api/run/")
                .trim_end_matches("/log");
            json(&log_de_run(store, id, &query))
        }
        "/api/metrics" => json(&metrics(store, &query)),
        "/api/alerts" => json(&alerts(store)),
        "/api/logs" => json(&logs(store)),
        _ => {
            if let Some(id) = path.strip_prefix("/api/run/") {
                return json(&run_detail(store, id));
            }
            (
                404,
                r#"{"error":"ruta desconocida"}"#.to_string(),
                "application/json",
            )
        }
    }
} // ---------- helpers ----------

/// Parte "/api/runs?limit=10" en (path, query).
fn split(path_query: &str) -> (String, String) {
    match path_query.find('?') {
        Some(i) => (path_query[..i].to_string(), path_query[i + 1..].to_string()),
        None => (path_query.to_string(), String::new()),
    }
}

/// Decodifica percent-encoding (`%XX`) y `+` de una query string.
///
/// La UI arma los links con `encodeURIComponent`, asi que un DAG con espacio o
/// acento llegaba como `mi%20dag` y se comparaba crudo contra el nombre real:
/// su landing no abria nunca (N-37).
fn url_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < b.len() => {
                let hex = std::str::from_utf8(&b[i + 1..i + 3]).ok();
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(v) => {
                        out.push(v);
                        i += 3;
                    }
                    None => {
                        out.push(b[i]);
                        i += 1;
                    }
                }
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Extrae un parámetro de la query string (?dag=x&y=1), ya decodificado.
fn query_param(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == key).then(|| url_decode(v))
    })
}

fn t_iso(ts: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp(ts, 0)
        .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "?".into())
}

/// Estado de una task como una sola palabra.
///
/// Con `{:?}` salia el Debug crudo de Rust (`Failed { retries_left: 0 }`), que
/// ademas no matcheaba ninguna clase CSS: la pill quedaba sin color.
fn task_state_str(s: &zeruel_sdk::TaskState) -> &'static str {
    match s {
        zeruel_sdk::TaskState::Pending => "Pending",
        zeruel_sdk::TaskState::Running => "Running",
        zeruel_sdk::TaskState::Success => "Success",
        zeruel_sdk::TaskState::Failed { .. } => "Failed",
    }
}

// ---------- API ----------

/// Resumen por DAG. Lo agrega SQLite con un GROUP BY: antes se traian los
/// 10.000 runs mas recientes a memoria y se contaban en Rust, en cada refresco
/// de cada pestaña abierta (N-36).
fn dags(store: &Store) -> serde_json::Value {
    let mut acc: std::collections::BTreeMap<String, serde_json::Value> =
        std::collections::BTreeMap::new();
    // Los contadores acumulados no contestan "¿esta roto AHORA?": un DAG con
    // 3 fallos de 52 se lee como sano aunque su ultima corrida haya fallado
    // hace media hora. Eso pasaba con 7 DAGs en una instalacion en produccion.
    let hist = store.historial_por_dag().unwrap_or_default();
    for d in store.dag_stats().unwrap_or_default() {
        let h = hist.get(&d.dag);
        acc.insert(
            d.dag.clone(),
            json!({
                "dag": d.dag, "runs": d.runs, "ok": d.ok, "fail": d.fail,
                "cancelled": d.cancelled, "running": d.running, "ultimo_run": d.ultimo_run,
                "ultimo_estado": h.and_then(|h| h.ultimo_estado.clone()),
                "racha_fallos": h.map(|h| h.racha_fallos).unwrap_or(0),
                "ultimas": h.map(|h| h.ultimas.clone()).unwrap_or_default(),
                // Arranca en false y lo prende el barrido de binarios de mas
                // abajo. Antes el campo quedaba AUSENTE para un DAG que solo
                // existe en el historial, y `x.desplegado === false` no lo
                // agarraba: un DAG dado de baja seguia contando como roto.
                "desplegado": false,
            }),
        );
    }
    // Aparecen aunque no hayan corrido nunca: primero los binarios desplegados
    // en el directorio de pipelines, despues los compilados en el binario.
    let vacio = |name: &str, desplegado: bool| {
        json!({"dag": name, "runs": 0, "ok": 0, "fail": 0, "cancelled": 0,
               "running": 0, "ultimo_run": null, "desplegado": desplegado,
               "ultimo_estado": null, "racha_fallos": 0, "ultimas": []})
    };
    // El cron y la proxima ejecucion salen del `--describe` del binario, que
    // es la unica fuente: la base guarda lo que paso, no lo que va a pasar.
    let ahora = chrono::Utc::now();
    let pausados = store.dags_pausados().unwrap_or_default();
    for p in pipelines_desplegados() {
        let e = acc
            .entry(p.desc.name.clone())
            .or_insert_with(|| vacio(&p.desc.name, true));
        if let Some(o) = e.as_object_mut() {
            o.insert("desplegado".into(), json!(true));
            o.insert("cron".into(), json!(p.desc.schedule));
            // Sin cron el pipeline solo corre cuando alguien lo encola: no hay
            // proxima que mostrar, y poner una seria mentir.
            let proxima = p
                .desc
                .schedule
                .as_deref()
                .and_then(|c| crate::cron::proxima(c, &ahora));
            o.insert("proxima".into(), json!(proxima.map(|t| t.timestamp())));
            o.insert("pausado".into(), json!(pausados.contains(&p.desc.name)));
        }
    }
    for name in crate::dags::names() {
        acc.entry(name.to_string())
            .or_insert_with(|| vacio(name, false));
    }
    json!(acc.values().collect::<Vec<_>>())
}

/// El grafo de un pipeline.
///
/// Primero se busca entre los binarios desplegados (su `--describe` es la
/// verdad: es el codigo que realmente va a correr); si no esta, se cae al
/// registro compilado.
fn graph(dag: Option<&str>) -> serde_json::Value {
    let name = dag.unwrap_or("ecommerce-etl");
    if let Some(p) = pipelines_desplegados()
        .into_iter()
        .find(|p| p.desc.name == name)
    {
        return crate::pipelines::grafo_json(&p.desc);
    }
    match crate::dags::graph_for(name) {
        Some(g) => g,
        None => json!({"name": name, "nodes": [], "edges": []}),
    }
}

/// Los pipelines que hay desplegados ahora mismo.
///
/// La web corre en otro proceso que `serve`, asi que tiene su propio registro.
/// El cache por fecha de modificacion evita preguntarle a cada binario en cada
/// refresco (cada `--describe` es un proceso, ~170 ms).
fn pipelines_desplegados() -> Vec<crate::pipelines::Pipeline> {
    use std::sync::{Mutex, OnceLock};
    static REG: OnceLock<Mutex<(crate::pipelines::Registro, std::path::PathBuf)>> = OnceLock::new();
    let celda =
        REG.get_or_init(|| Mutex::new((crate::pipelines::Registro::new(), dir_pipelines())));
    let mut g = match celda.lock() {
        Ok(g) => g,
        Err(e) => e.into_inner(), // un panic previo no deja la UI sin grafo
    };
    let dir = g.1.clone();
    g.0.escanear(&dir)
}

/// Donde busca la web los binarios de pipeline.
///
/// Lo fija `run()` desde el flag `--pipelines`. Antes se resolvia contra el
/// directorio actual del proceso, y eso hacia que la web listara los pipelines
/// de donde se la hubiera ejecutado en vez de los del despliegue: los DAGs
/// desplegados no aparecian, y en su lugar quedaban los compilados, que no
/// tienen binario y por lo tanto no corren. Un "ejecutar" que no hace nada y
/// no explica por que.
static DIR_PIPELINES: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();

fn dir_pipelines() -> std::path::PathBuf {
    DIR_PIPELINES
        .get()
        .cloned()
        .unwrap_or_else(|| std::path::PathBuf::from("pipelines"))
}

/// El `zeruel.toml` de este proceso, si lo hay.
static RUTA_CONFIG: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();

/// Qué dice el archivo y qué está usando el scheduler de verdad.
///
/// Los dos lados existen porque **no siempre coinciden**, y esa diferencia es
/// justo la que no se podía ver: un servicio registrado sin `--config` arranca
/// con los valores de fábrica, el archivo sigue diciendo otra cosa, y desde
/// afuera se ve idéntico a una instalación sana. Costó dos días.
///
/// El panel no edita: el servicio web tiene permiso de sólo lectura sobre su
/// configuración a propósito. Para cambiarla está `zeruel config set`, que corre
/// con los permisos de quien lo ejecuta.
fn configuracion(store: &Store) -> serde_json::Value {
    // Con qué está corriendo el scheduler: lo publica él al arrancar.
    let efectivo: Option<serde_json::Value> = store
        .meta_get(crate::scheduler::CLAVE_EFECTIVO)
        .ok()
        .flatten()
        .and_then(|s| serde_json::from_str(&s).ok());

    // Qué dice el archivo.
    let ruta = RUTA_CONFIG.get().cloned();
    let doc: Option<toml::Value> = ruta
        .as_ref()
        .and_then(|p| std::fs::read_to_string(p).ok())
        .and_then(|t| toml::from_str(&t).ok());

    let ajustes: Vec<serde_json::Value> = crate::configurar::AJUSTES
        .iter()
        .map(|a| {
            let en_archivo = doc
                .as_ref()
                .and_then(|d| d.get(a.seccion))
                .and_then(|s| s.get(a.clave))
                .map(|v| match v {
                    toml::Value::String(s) => s.clone(),
                    otro => otro.to_string(),
                });
            // El efectivo se compara sólo para lo que el scheduler publica; el
            // resto (los ajustes de la web) no tiene con qué contrastarse.
            let en_uso = efectivo
                .as_ref()
                .and_then(|e| e.get(clave_efectiva(a.clave)))
                .map(|v| v.to_string().trim_matches('"').to_string());
            serde_json::json!({
                "clave": a.clave,
                "seccion": a.seccion,
                "que_hace": a.que_hace,
                "aplica": a.aplica,
                "archivo": en_archivo,
                "en_uso": en_uso,
            })
        })
        .collect();

    serde_json::json!({
        "archivo": ruta.map(|p| p.display().to_string()),
        "scheduler": efectivo,
        "ajustes": ajustes,
    })
}

/// El nombre del ajuste en el archivo no siempre es el que publica el
/// scheduler: `ram_max` es texto ("2500M") y lo que corre es un número de MB.
fn clave_efectiva(clave: &str) -> &str {
    match clave {
        "ram_max" => "ram_max_mb",
        otra => otra,
    }
}

/// Corridas, con todo el recorte hecho en SQL.
///
/// Antes traia las 100 mas recientes y la interfaz filtraba sobre esas 100.
/// Medido en una instalacion en produccion: filtrar por "fallo" devolvia VACIO con 51
/// fallos guardados, porque las 100 ultimas cubrian una hora y media sin
/// ninguno. Una tabla que contesta "no hay fallos" cuando los hay es peor que
/// una tabla vacia.
fn runs(store: &Store, query: &str) -> serde_json::Value {
    let num = |k: &str| query_param(query, k).and_then(|v| v.parse::<i64>().ok());
    let txt = |k: &str| {
        query_param(query, k)
            .map(|v| url_decode(&v))
            .filter(|v| !v.is_empty())
    };

    let f = zeruel_core::corridas::Filtro {
        texto: txt("q"),
        dag: txt("dag"),
        estado: txt("estado"),
        desde: num("desde"),
        hasta: num("hasta"),
        dur_min_secs: num("dur_min"),
        ram_min_mb: num("ram_min"),
        con_reintentos: query_param(query, "reintentos").as_deref() == Some("1"),
        limite: num("limite").unwrap_or(50).clamp(1, 500) as usize,
        offset: num("offset").unwrap_or(0).max(0) as usize,
        orden: txt("orden"),
        // Por defecto lo mas nuevo arriba, que es lo que se mira al entrar.
        desc: query_param(query, "dir").as_deref() != Some("asc"),
    };

    match store.corridas(&f) {
        Ok(p) => json!({
            "filas": p.filas.iter().map(|r| {
                let mut v = serde_json::to_value(r).unwrap_or_else(|_| json!({}));
                // La interfaz muestra la hora ya formateada: hacerlo aca evita
                // que cada vista la arme distinto.
                v["started_iso"] = json!(t_iso(r.started_at));
                v
            }).collect::<Vec<_>>(),
            "total": p.total,
            "offset": p.offset,
            "limite": p.limite,
            "dags": store.dags_con_corridas().unwrap_or_default(),
        }),
        Err(e) => json!({"error": format!("{e:#}"), "filas": [], "total": 0}),
    }
}

/// Detalle de un run por id. Va directo a la DB (`run_by_id`): antes se traian
/// los 200 runs mas recientes y se buscaba con un `find` lineal, asi que un run
/// mas viejo devolvia "no encontrado" aunque estuviera guardado (N-13).
fn run_detail(store: &Store, id: &str) -> serde_json::Value {
    let uuid = match uuid::Uuid::parse_str(id) {
        Ok(u) => u,
        Err(_) => return json!({"error": "id de run invalido", "id": id}),
    };
    match store.run_by_id(uuid).ok().flatten() {
        Some(r) => {
            let tasks = store.task_runs(r.id).unwrap_or_default();
            let tarr: Vec<serde_json::Value> = tasks
                .iter()
                .map(|t| {
                    json!({
                        "task": t.task,
                        "state": task_state_str(&t.state),
                        "rows": t.rows,
                        "message": t.message,
                        "ram_peak_mb": t.ram_peak_mb,
                        "cpu_secs": t.cpu_secs,
                        "retries_left": t.retries_left,
                        "intentos": t.intentos,
                        "reused": t.reused,
                        // Hacen falta para dibujar la linea de tiempo del run:
                        // sin el arranque de cada tarea no se puede saber si
                        // corrieron en paralelo o una despues de la otra.
                        "started_at": t.started_at,
                        "finished_at": t.finished_at,
                        "dur_secs": dur_secs(t.started_at, t.finished_at),
                    })
                })
                .collect();
            json!({
                "id": r.id,
                "dag": r.dag,
                "param": r.param,
                "state": format!("{:?}", r.state),
                "started_at": r.started_at,
                "started_iso": t_iso(r.started_at),
                "finished_at": r.finished_at,
                "finished_iso": r.finished_at.map(t_iso),
                "dur_secs": dur_secs(Some(r.started_at), r.finished_at),
                "tasks": tarr,
            })
        }
        None => json!({"error": "run no encontrado", "id": id}),
    }
}

/// Duracion en segundos. Si no termino, cuenta contra el reloj actual: asi la
/// UI puede mostrar cuanto lleva corriendo un run en vivo.
fn dur_secs(desde: Option<i64>, hasta: Option<i64>) -> Option<i64> {
    let d = desde?;
    Some((hasta.unwrap_or_else(now_unix) - d).max(0))
}

fn queue(store: &Store) -> serde_json::Value {
    let q = store.queue_items().unwrap_or_default();
    let sla = store.sla_config().unwrap_or_default();
    let now = now_unix();
    let items: Vec<serde_json::Value> = q
        .iter()
        .map(|i| {
            json!({
                "id": i.id,
                "dag": i.dag,
                "param": i.param,
                "priority": i.priority,
                "state": i.state,
                "enqueued_iso": t_iso(i.enqueued_at),
                "espera_seg": (now - i.enqueued_at).max(0),
                "max_seg": sla.get(&i.dag).copied(),
            })
        })
        .collect();
    json!({"items": items, "max_size": crate::ops::get_queue_max(store)})
}

fn hygiene(store: &Store) -> serde_json::Value {
    match store.hygiene() {
        Ok(h) => serde_json::to_value(h).unwrap_or(json!({})),
        Err(e) => json!({"error": e.to_string()}),
    }
}

/// Agregados para la tab de Metricas: rollups mensuales, stats por task, SLAs.
/// Arma el filtro de metricas desde la query string.
///
/// El default es **hoy**, no todo: abrir la pestaña tiene que contestar "¿como
/// venimos?" y ser barato, no volcar el historico entero.
fn filtro_metricas(query: &str) -> zeruel_core::metricas::Filtro {
    let num = |k: &str| query_param(query, k).and_then(|v| v.parse::<i64>().ok());
    let ahora = now_unix();

    // `dias` es el atajo de la UI (hoy = 1, semana = 7...). `desde`/`hasta` en
    // unix lo pisan, que es lo que usa el rango a mano.
    let (desde, hasta) = match (num("desde"), num("hasta")) {
        (Some(d), Some(h)) => (d, h),
        (Some(d), None) => (d, ahora + 1),
        _ => {
            let dias = num("dias").unwrap_or(1).clamp(1, 3650);
            // Desde la medianoche local de hace `dias-1` dias: "hoy" tiene que
            // ser el dia calendario, no las ultimas 24 horas.
            (medianoche_local(ahora) - (dias - 1) * 86_400, ahora + 1)
        }
    };

    zeruel_core::metricas::Filtro {
        desde,
        hasta,
        dag: query_param(query, "dag")
            .map(|d| url_decode(&d))
            .filter(|d| !d.is_empty()),
        ram_min_mb: num("ram_min"),
        dur_min_secs: num("dur_min"),
        solo_fallos: query_param(query, "fallos").as_deref() == Some("1"),
    }
}

/// La medianoche local del dia que contiene `t`.
fn medianoche_local(t: i64) -> i64 {
    use chrono::{Local, TimeZone};
    Local
        .timestamp_opt(t, 0)
        .single()
        .and_then(|dt| dt.date_naive().and_hms_opt(0, 0, 0))
        .and_then(|nd| Local.from_local_datetime(&nd).single())
        .map(|dt| dt.timestamp())
        .unwrap_or(t - t.rem_euclid(86_400))
}

fn metrics(store: &Store, query: &str) -> serde_json::Value {
    let f = filtro_metricas(query);
    let sla = store.sla_config().unwrap_or_default();
    json!({
        "filtro": {
            "desde": f.desde, "hasta": f.hasta, "dag": f.dag,
            "ram_min": f.ram_min_mb, "dur_min": f.dur_min_secs,
            "fallos": f.solo_fallos,
        },
        "volumen":    store.met_volumen(&f).unwrap_or_default(),
        "dags":       store.met_por_dag(&f).unwrap_or_default(),
        "tareas":     store.met_por_tarea(&f).unwrap_or_default(),
        "actividad":  store.met_actividad(&f).unwrap_or_default(),
        // Para el autocompletado del buscador: los DAGs que existen en el
        // periodo, no los 47 que existieron alguna vez.
        "dags_conocidos": store.met_dags(&f).unwrap_or_default(),
        // La columna `filas` estaba en 0 en las 2.050 filas de la instalacion
        // real. Una columna que siempre dice 0 entrena a ignorar la tabla
        // entera, asi que se muestra solo si algo la llena.
        "hay_filas": store.met_hay_filas(&f).unwrap_or(false),
        "sla": sla,
    })
}

/// Alertas activas: violaciones SLA en cola, fails recientes (24h), warning de DB.
fn alerts(store: &Store) -> serde_json::Value {
    let h = store.hygiene().unwrap_or_default();
    let q = store.queue_items().unwrap_or_default();
    let sla = store.sla_config().unwrap_or_default();
    let now = now_unix();
    let mut sla_viol: Vec<serde_json::Value> = Vec::new();
    for it in &q {
        if let Some(max) = sla.get(&it.dag) {
            let espera = now - it.enqueued_at;
            if espera > *max && store.alerta_vista(&format!("cola:{}", it.id)).is_none() {
                sla_viol.push(json!({
                    "id": it.id, "dag": it.dag, "param": it.param,
                    "espera_seg": espera, "max_seg": max,
                }));
            }
        }
    }
    // El filtro va en SQL (N-36): antes se traian 2.000 runs a memoria en cada
    // refresco para quedarse con los de un dia.
    let fails_24h: Vec<serde_json::Value> = store
        .failed_since(now - 86_400)
        .unwrap_or_default()
        .iter()
        // Las cerradas no vuelven: una corrida es inmutable, asi que si el DAG
        // falla de nuevo sera otra corrida, con otro id, y otra alerta.
        .filter(|r| store.alerta_vista(&r.id.to_string()).is_none())
        .map(|r| {
            json!({"id": r.id, "dag": r.dag, "param": r.param, "started_iso": t_iso(r.started_at)})
        })
        .collect();
    let db_silenciada = store.alerta_vista("db").is_some_and(|t| now - t < 86_400);
    let db_warning = h.db_mb_warning && !db_silenciada;
    // Un mismo error en varios DAGs distintos no son N fallos: es UNO, del
    // entorno. Reportarlos de a uno hace perder horas buscando que tienen en
    // comun, teniendo el dato en la base desde el primer minuto.
    //
    // Se piden 2 DAGs distintos como minimo: con uno solo es un pipeline con un
    // bug, y eso ya se ve en su tira de corridas del panel.
    let sistemicos = store
        .fallos_sistemicos(now - 21_600, 2)
        .unwrap_or_default()
        .into_iter()
        .filter(|f| store.alerta_vista(&clave_sistemico(&f.error)).is_none())
        .collect::<Vec<_>>();
    // Una cola que crece de forma sostenida es un sintoma que NO depende de por
    // que crece: scheduler mal configurado, pipeline lento, cron nuevo. En la
    // instalacion en produccion llego a 1.400 items con 22 h de espera antes de que
    // alguien lo notara, y la causa no se parecia a nada de lo que se buscaba.
    //
    // Ventana de 6 h, 2 h de espera y 50 items: un backfill normal no llega a
    // eso, y un atraso de verdad lo pasa largo.
    let atraso = store
        .cola_atrasada(6 * 3600, 2 * 3600, 50)
        .unwrap_or(None)
        .filter(|_| store.alerta_vista("cola-atrasada").is_none());
    let totales = sla_viol.len() as u64
        + fails_24h.len() as u64
        + sistemicos.len() as u64
        + u64::from(atraso.is_some())
        + u64::from(db_warning);
    json!({
        "sla_violaciones": sla_viol,
        "fails_24h": fails_24h,
        "sistemicos": sistemicos.iter().map(|f| json!({
            "clave": clave_sistemico(&f.error),
            "error": f.error, "veces": f.veces, "dags": f.dags,
            "ejemplos": f.ejemplos, "desde_iso": t_iso(f.desde),
        })).collect::<Vec<_>>(),
        "cola_atrasada": atraso,
        "db_warning": db_warning,
        "db_mb": h.db_size_bytes as f64 / 1048576.0,
        "cola": q.len() as u64,
        "totales": totales,
    })
}

/// Ultimas lineas del log mas nuevo de state/logs (si existe).
fn logs(store: &Store) -> serde_json::Value {
    let dir = store.path.parent().unwrap_or(Path::new("."));
    let logs_dir = dir.join("logs");
    let files: Vec<PathBuf> = std::fs::read_dir(&logs_dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .map(|n| n.starts_with("zeruel.log."))
                        .unwrap_or(false)
                })
                .collect()
        })
        .unwrap_or_default();
    match files
        .iter()
        .max_by_key(|p| p.metadata().and_then(|m| m.modified()).ok())
    {
        Some(p) => match std::fs::read_to_string(p) {
            Ok(content) => {
                let lines: Vec<&str> = content.lines().rev().take(200).collect();
                json!({"archivo": p.display().to_string(), "lineas": lines})
            }
            Err(e) => json!({"error": e.to_string()}),
        },
        None => json!({"error": "sin logs en state/logs"}),
    }
}

/// Retry con un click: encola de nuevo el ultimo run fallido (mismo dag+param).
/// Las ultimas lineas del log de una corrida.
///
/// Se lee **desde el final**, no entero. Una tarea de dos horas deja logs de
/// decenas de MB y lo que se busca esta siempre al final; cargar el archivo
/// completo para mostrar 200 lineas es la forma de que la interfaz se cuelgue
/// justo cuando mas se la necesita.
fn log_de_run(store: &Store, id: &str, query: &str) -> serde_json::Value {
    let Ok(uuid) = uuid::Uuid::parse_str(id) else {
        return json!({"error": "el id no es un uuid"});
    };
    let n: usize = query_param(query, "n")
        .and_then(|v| v.parse().ok())
        .unwrap_or(200)
        .clamp(1, 5000);

    let dir = store.path.parent().unwrap_or(Path::new(".")).join("logs");
    let archivo = dir.join(format!("run-{uuid}.log"));
    let Ok(meta) = std::fs::metadata(&archivo) else {
        // Que no exista es normal mientras el run espera en cola: el archivo lo
        // crea el subproceso al arrancar.
        return json!({
            "lineas": [],
            "existe": false,
            "nota": "todavia no hay log: el run no arranco, o ya se podo",
        });
    };

    match ultimas_lineas(&archivo, meta.len(), n) {
        Ok((lineas, truncado)) => json!({
            "lineas": lineas,
            "existe": true,
            "truncado": truncado,
            "bytes": meta.len(),
            "archivo": archivo.display().to_string(),
        }),
        Err(e) => json!({"error": format!("no pude leer el log: {e}")}),
    }
}

/// Saca los codigos de color ANSI.
///
/// El log de un run es el stdout del subproceso redirigido a un archivo, y el
/// proceso escribe con colores porque no sabe que no hay una terminal del otro
/// lado. En el navegador eso se ve como basura. Se limpia al servir y no al
/// escribir: el archivo conserva exactamente lo que el proceso dijo.
fn sin_ansi(linea: &str) -> String {
    let mut out = String::with_capacity(linea.len());
    let mut cs = linea.chars();
    while let Some(c) = cs.next() {
        if c != '\u{1b}' {
            out.push(c);
            continue;
        }
        // Una secuencia CSI termina en una letra: se descarta hasta ahi. Si el
        // archivo se corto a la mitad de una, el iterador se agota y sale.
        for f in cs.by_ref() {
            if f.is_ascii_alphabetic() {
                break;
            }
        }
    }
    out
}

/// Lee las ultimas `n` lineas leyendo solo la cola del archivo.
///
/// Se estiman 200 bytes por linea y se lee un bloque desde el final; si no
/// alcanzo, se duplica. Asi un log de 27 MB cuesta leer unos pocos KB.
fn ultimas_lineas(ruta: &Path, largo: u64, n: usize) -> std::io::Result<(Vec<String>, bool)> {
    use std::io::{Read, Seek, SeekFrom};

    let mut f = std::fs::File::open(ruta)?;
    let mut bloque = (n as u64 * 200).max(8 * 1024);

    loop {
        let desde = largo.saturating_sub(bloque);
        f.seek(SeekFrom::Start(desde))?;
        let mut buf = Vec::with_capacity((largo - desde) as usize);
        // Por referencia: `take` consume el File, y el bucle puede necesitar otra vuelta.
        (&f).take(largo - desde).read_to_end(&mut buf)?;

        let texto = String::from_utf8_lossy(&buf);
        // Si se empezo a mitad del archivo, la primera linea puede estar
        // cortada: se descarta para no mostrar medio renglon.
        let mut it: Vec<&str> = texto.lines().collect();
        if desde > 0 && !it.is_empty() {
            it.remove(0);
        }
        if it.len() >= n || desde == 0 {
            let total = it.len();
            let lineas: Vec<String> = it
                .into_iter()
                .skip(total.saturating_sub(n))
                .map(sin_ansi)
                .collect();
            return Ok((lineas, desde > 0));
        }
        bloque *= 4;
    }
}

/// Cierra una alerta.
fn cerrar_alerta(store: &Store, clave: &str) -> serde_json::Value {
    if clave.is_empty() {
        return json!({"ok": false, "motivo": "falta la clave de la alerta"});
    }
    match store.cerrar_alerta(clave, now_unix()) {
        Ok(()) => json!({"ok": true, "msg": "alerta cerrada"}),
        Err(e) => json!({"ok": false, "motivo": e.to_string()}),
    }
}

/// Cierra todas las que se ven ahora.
///
/// Con veinte alertas, cerrarlas de a una es trabajo manual sin sentido. Cierra
/// solo las que estan a la vista en este momento: una que aparezca despues no
/// queda silenciada de antemano.
/// Saca un item de la cola, esté corriendo o esperando.
///
/// Si ya arrancó, la cancelación viaja por la base y la levanta el scheduler en
/// su próximo tick, que es lo mismo que hace el botón de la tabla de corridas.
/// La respuesta lo dice, porque el efecto no es inmediato y sin avisar parece
/// que el botón no hizo nada.
fn cancelar_cola(store: &Store, id_str: &str) -> serde_json::Value {
    let id = match id_str.parse::<i64>() {
        Ok(v) => v,
        Err(_) => return json!({"ok": false, "error": format!("id de cola invalido: {id_str}")}),
    };
    match store.cancelar_item_cola(id) {
        Ok(Some(run)) => json!({
            "ok": true, "run": run.to_string(),
            "msg": "cancelación pedida: el scheduler corta el proceso en su próximo tick"
        }),
        Ok(None) => json!({"ok": true, "msg": "sacado de la cola antes de que arrancara"}),
        Err(e) => json!({"ok": false, "error": format!("{e:#}")}),
    }
}

/// La clave con la que se cierra una alerta de fallo sistemico.
///
/// Sale del texto del error y no de un id, porque el "mismo problema" puede
/// seguir apareciendo en corridas nuevas: cerrarlo por id lo haria volver en la
/// siguiente. Se acorta para que quepa como clave y para que un mensaje con una
/// ruta larga no genere una clave distinta cada vez.
fn clave_sistemico(error: &str) -> String {
    let corto: String = error.chars().take(60).collect();
    format!("sis:{}", corto.trim())
}

fn cerrar_todas(store: &Store) -> serde_json::Value {
    let ahora = now_unix();
    let mut n = 0usize;

    for r in store.failed_since(ahora - 86_400).unwrap_or_default() {
        if store.cerrar_alerta(&r.id.to_string(), ahora).is_ok() {
            n += 1;
        }
    }
    let sla = store.sla_config().unwrap_or_default();
    for it in store.queue_items().unwrap_or_default() {
        if let Some(max) = sla.get(&it.dag) {
            if ahora - it.enqueued_at > *max
                && store
                    .cerrar_alerta(&format!("cola:{}", it.id), ahora)
                    .is_ok()
            {
                n += 1;
            }
        }
    }
    if store.hygiene().map(|h| h.db_mb_warning).unwrap_or(false) {
        let _ = store.cerrar_alerta("db", ahora);
        n += 1;
    }
    json!({"ok": true, "msg": format!("{n} alerta(s) cerradas")})
}

/// El codigo empotrado del binario que expone este DAG.
fn codigo(dag: Option<&str>) -> serde_json::Value {
    let Some(dag) = dag else {
        return json!({"error": "falta ?dag="});
    };
    let Some(p) = pipelines_desplegados()
        .into_iter()
        .find(|p| p.desc.name == dag)
    else {
        return json!({"error": format!("no hay un binario desplegado para '{dag}'")});
    };
    match crate::pipelines::fuente(&p) {
        Ok(v) => v,
        Err(e) => json!({"error": format!("{e:#}"), "bin": p.bin.display().to_string()}),
    }
}

/// Pausa o activa un DAG.
fn pausar(store: &Store, dag: &str, pausar: bool) -> serde_json::Value {
    if dag.is_empty() {
        return json!({"ok": false, "motivo": "falta el nombre del DAG"});
    }
    let r = if pausar {
        store.pausar_dag(dag)
    } else {
        store.activar_dag(dag)
    };
    match r {
        Ok(()) => json!({
            "ok": true,
            "pausado": pausar,
            "msg": if pausar {
                format!("{dag} pausado: no se agenda mas. Lo que ya esta en cola o corriendo sigue.")
            } else {
                format!("{dag} activado: vuelve a su horario")
            }
        }),
        Err(e) => json!({"ok": false, "motivo": e.to_string()}),
    }
}

fn retry(store: &Store) -> serde_json::Value {
    let runs = store.recent_runs(100, 0).unwrap_or_default();
    match runs
        .iter()
        .find(|r| matches!(r.state, zeruel_core::RunState::Failed))
    {
        Some(r) => match store.enqueue(&r.dag, &r.param, 0) {
            Ok(res) if res.ok => {
                json!({"ok": true, "msg": format!("encolado retry de {}/{}", r.dag, r.param)})
            }
            Ok(res) => json!({"ok": false, "motivo": res.motivo}),
            Err(e) => json!({"ok": false, "motivo": e.to_string()}),
        },
        None => json!({"ok": false, "motivo": "no hay runs fallidos recientes"}),
    }
}

fn read_body(request: &mut tiny_http::Request) -> String {
    let mut buf = String::new();
    let _ = request.as_reader().read_to_string(&mut buf);
    buf
}

/// POST /api/run: encola un run nuevo. Body JSON {dag, param, priority}
fn enqueue(store: &Store, body: &str) -> serde_json::Value {
    let v: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return json!({"ok": false, "motivo": format!("json invalido: {e}")}),
    };
    let dag = v
        .get("dag")
        .and_then(|d| d.as_str())
        .unwrap_or("ecommerce-etl");
    let param = v.get("param").and_then(|d| d.as_str()).unwrap_or("");
    let prio = v.get("priority").and_then(|d| d.as_i64()).unwrap_or(0);
    match store.enqueue(dag, param, prio) {
        Ok(res) if res.ok => json!({"ok": true, "msg": format!("encolado {dag} {param}")}),
        Ok(res) => json!({"ok": false, "motivo": res.motivo}),
        Err(e) => json!({"ok": false, "motivo": e.to_string()}),
    }
}

fn cancel_run(store: &Store, id: uuid::Uuid) -> serde_json::Value {
    if store.request_cancel(id) {
        json!({"ok": true, "msg": format!("cancelando run {id}")})
    } else {
        json!({"ok": false, "motivo": "el run no esta corriendo (o ya termino)"})
    }
} // ---------- UI ----------

// La UI vive en `ui.html`, al lado de este archivo, y se empotra en el binario
// con include_str! (N-17). Antes eran ~280 lineas de HTML+CSS+JS dentro de un
// `const` de Rust: sin resaltado, sin linter, y ahi se colaron los bugs B-06 a
// B-08. El binario sigue siendo uno solo: include_str! es en tiempo de compilacion.
const UI_HTML: &str = include_str!("ui.html");

#[cfg(test)]
mod tests {
    use super::*;
    use zeruel_core::store::{RunState, TaskStateRecord};
    use zeruel_sdk::TaskState;

    fn store() -> Store {
        Store::open_in_memory().unwrap()
    }

    /// Pide una ruta y devuelve el JSON de la respuesta.
    fn get(store: &Store, ruta: &str) -> serde_json::Value {
        let (status, body, ctype) = handle(store, "GET", ruta, "");
        assert_eq!(status, 200, "GET {ruta} devolvio {status}: {body}");
        assert_eq!(ctype, "application/json");
        serde_json::from_str(&body).expect("respuesta JSON valida")
    }

    fn post_json(store: &Store, ruta: &str, body: &str) -> serde_json::Value {
        let (_s, b, _c) = handle(store, "POST", ruta, body);
        serde_json::from_str(&b).expect("respuesta JSON valida")
    }

    #[test]
    fn la_comparacion_de_token_no_corta_en_la_primera_diferencia() {
        assert!(iguales_en_tiempo_constante(b"secreto", b"secreto"));
        assert!(!iguales_en_tiempo_constante(b"secreto", b"secretO"));
        assert!(!iguales_en_tiempo_constante(b"secreto", b"secret"));
        assert!(!iguales_en_tiempo_constante(b"", b"x"));
        assert!(iguales_en_tiempo_constante(b"", b""));
        // El caso que importa: dos tokens que comparten el principio no
        // pueden distinguirse por cuanto tarda la comparacion.
        assert!(!iguales_en_tiempo_constante(b"aaaaaaaaX", b"aaaaaaaaY"));
    }

    #[test]
    fn url_decode_maneja_espacios_y_acentos() {
        // N-37
        assert_eq!(url_decode("mi%20dag"), "mi dag");
        assert_eq!(url_decode("mi+dag"), "mi dag");
        assert_eq!(url_decode("a%C3%B1o"), "año");
        assert_eq!(url_decode("simple"), "simple");
        assert_eq!(url_decode("100%"), "100%", "un % suelto no rompe");
    }

    #[test]
    fn query_param_decodifica() {
        let (_p, q) = split("/api/graph?dag=mi%20dag&x=1");
        assert_eq!(query_param(&q, "dag").as_deref(), Some("mi dag"));
        assert_eq!(query_param(&q, "x").as_deref(), Some("1"));
        assert_eq!(query_param(&q, "nope"), None);
    }

    #[test]
    fn split_separa_path_y_query() {
        assert_eq!(split("/api/runs"), ("/api/runs".into(), "".into()));
        assert_eq!(split("/api/runs?a=1"), ("/api/runs".into(), "a=1".into()));
    }

    /// Sin scheduler publicando, la pestaña no puede afirmar qué se está
    /// usando. Tiene que decir que no sabe, y no mostrar el archivo como si
    /// fuera lo que corre: esa confusión es exactamente el bug que originó
    /// esta pantalla.
    #[test]
    fn sin_scheduler_la_configuracion_no_inventa_lo_que_esta_en_uso() {
        let s = store();
        let v = get(&s, "/api/config");
        assert!(
            v["scheduler"].is_null(),
            "no hay scheduler que haya publicado"
        );
        let ajustes = v["ajustes"].as_array().unwrap();
        assert!(
            !ajustes.is_empty(),
            "la tabla de ajustes tiene que venir igual"
        );
        assert!(
            ajustes.iter().all(|a| a["en_uso"].is_null()),
            "sin dato del scheduler, 'en uso' no puede tener valores"
        );
    }

    /// El caso que costó dos días: el archivo dice una cosa y el scheduler
    /// corre con otra. Las dos tienen que llegar al panel por separado para
    /// que la diferencia se pueda ver.
    #[test]
    fn la_configuracion_muestra_lo_que_el_scheduler_publico() {
        let s = store();
        s.meta_set(
            crate::scheduler::CLAVE_EFECTIVO,
            r#"{"paralelo":2,"workers":4,"config_origen":null,"pid":123}"#,
        )
        .unwrap();

        let v = get(&s, "/api/config");
        assert_eq!(v["scheduler"]["paralelo"], 2);
        assert!(
            v["scheduler"]["config_origen"].is_null(),
            "arrancó sin archivo, y eso es lo que hay que poder ver"
        );
        let paralelo = v["ajustes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["clave"] == "paralelo")
            .expect("paralelo tiene que estar en la tabla");
        assert_eq!(paralelo["en_uso"], "2");
    }

    #[test]
    fn ruta_desconocida_da_404() {
        let s = store();
        let (status, _b, _c) = handle(&s, "GET", "/api/no-existe", "");
        assert_eq!(status, 404);
    }

    #[test]
    fn la_ui_se_sirve_como_html() {
        let s = store();
        let (status, body, ctype) = handle(&s, "GET", "/", "");
        assert_eq!(status, 200);
        assert!(ctype.starts_with("text/html"));
        assert!(body.contains("<title>zeruel</title>"));
        // La UI escapa lo que viene del server: la funcion tiene que estar.
        assert!(
            body.contains("function esc("),
            "la UI tiene que escapar (N-32)"
        );
    }

    #[test]
    fn dags_incluye_los_registrados_sin_runs() {
        let s = store();
        let v = get(&s, "/api/dags");
        let arr = v.as_array().unwrap();
        assert!(
            !arr.is_empty(),
            "los DAGs del registry aparecen aunque no corrieron"
        );
        assert!(arr.iter().any(|d| d["dag"] == "ecommerce-etl"));
        assert_eq!(arr[0]["runs"], 0);
    }

    #[test]
    fn dags_cuenta_cancelados_aparte() {
        // N-01: un run cancelado no puede figurar como "en curso".
        let s = store();
        let id = s.create_run("ecommerce-etl", "p").unwrap();
        s.request_cancel(id);
        let v = get(&s, "/api/dags");
        let d = v
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["dag"] == "ecommerce-etl")
            .unwrap()
            .clone();
        assert_eq!(d["cancelled"], 1);
        assert_eq!(d["running"], 0);
    }

    /// Un DAG que ya no esta desplegado —se le borro el pipeline— tiene que
    /// decirlo explicitamente, no dejar el campo ausente.
    ///
    /// El panel lo usa para no contarlo como "roto ahora": un DAG dado de baja
    /// no se arregla, y mandar a revisarlo hace perder el tiempo. Con el campo
    /// ausente, `desplegado === false` no lo agarraba y seguia apareciendo.
    #[test]
    fn un_dag_solo_en_el_historial_se_marca_como_no_desplegado() {
        let s = store();
        let r = s.create_run("dado-de-baja", "p").unwrap();
        s.set_run_state(r, zeruel_core::RunState::Failed).unwrap();

        let v = get(&s, "/api/dags");
        let d = v
            .as_array()
            .unwrap()
            .iter()
            .find(|x| x["dag"] == "dado-de-baja")
            .expect("tiene que aparecer, con su historial");
        assert_eq!(d["desplegado"], false, "explicito, no ausente: {d}");
        assert_eq!(d["ultimo_estado"], "Failed", "y su ultimo estado igual");
    }

    #[test]
    fn runs_expone_estado_y_duracion() {
        let s = store();
        let id = s.create_run("ecommerce-etl", "P1").unwrap();
        s.set_run_state(id, RunState::Success).unwrap();
        let v = get(&s, "/api/runs");
        let r = &v["filas"].as_array().unwrap()[0];
        assert_eq!(r["param"], "P1");
        assert_eq!(r["state"], "Success");
        assert!(r["dur_secs"].is_i64(), "la UI muestra duracion");
        assert_eq!(v["total"], 1, "y el total, para poder paginar");
    }

    /// **El bug de la pestaña Corridas.** Con mas corridas que una pagina,
    /// filtrar por estado tiene que mirar toda la historia. Medido en la
    /// instalacion en produccion: la pestaña decia "no hay fallos" con 51 guardados,
    /// porque las 100 ultimas cubrian hora y media sin ninguno.
    #[test]
    fn filtrar_por_estado_no_se_queda_en_la_primera_pagina() {
        let s = store();
        let viejo = s.create_run("d", "el-fallo").unwrap();
        s.set_run_state(viejo, RunState::Failed).unwrap();
        for i in 0..80 {
            let r = s.create_run("d", &format!("ok-{i}")).unwrap();
            s.set_run_state(r, RunState::Success).unwrap();
        }
        // Una pagina de 50 no llega al fallo, que quedo 81 lugares atras.
        let v = get(&s, "/api/runs?estado=Failed&limite=50");
        assert_eq!(v["total"], 1, "tiene que encontrarlo: {v}");
        assert_eq!(v["filas"][0]["param"], "el-fallo");
    }

    /// La fila trae lo que hace falta para decidir si abrirla, sin abrirla.
    #[test]
    fn la_fila_de_corrida_trae_ram_y_tareas() {
        let s = store();
        let r = s.create_run("d", "p").unwrap();
        s.set_task_state(TaskStateRecord::con_intentos(
            r,
            "t",
            &TaskState::Success,
            0,
            "ok",
            333,
            1.0,
            2,
        ))
        .unwrap();
        s.set_run_state(r, RunState::Success).unwrap();
        let v = get(&s, "/api/runs");
        let f = &v["filas"][0];
        assert_eq!(f["ram_peak_mb"], 333);
        assert_eq!(f["tareas"], 1);
        assert_eq!(f["reintentadas"], 1);
    }

    /// El orden llega por la query y va a un ORDER BY, donde no se pueden usar
    /// parametros: una columna inventada tiene que ignorarse, no ejecutarse.
    #[test]
    fn un_orden_inventado_no_rompe_ni_ejecuta_nada() {
        let s = store();
        let r = s.create_run("d", "p").unwrap();
        s.set_run_state(r, RunState::Success).unwrap();
        let v = get(&s, "/api/runs?orden=x;%20DROP%20TABLE%20runs;--");
        assert_eq!(v["total"], 1, "cae al orden por defecto: {v}");
        assert_eq!(get(&s, "/api/runs")["total"], 1, "la tabla sigue ahi");
    }

    #[test]
    fn run_detail_encuentra_un_run_viejo() {
        // N-13: antes solo miraba los 200 mas recientes.
        let s = store();
        let viejo = s.create_run("ecommerce-etl", "viejo").unwrap();
        s.set_task_state(TaskStateRecord::medido(
            viejo,
            "validar",
            &TaskState::Success,
            99,
            "ok",
            10,
            1.0,
        ))
        .unwrap();
        for i in 0..250 {
            s.create_run("ecommerce-etl", &format!("otro{i}")).unwrap();
        }
        let v = get(&s, &format!("/api/run/{viejo}"));
        assert_eq!(v["param"], "viejo");
        assert_eq!(v["tasks"][0]["task"], "validar");
        assert_eq!(v["tasks"][0]["rows"], 99);
    }

    #[test]
    fn run_detail_con_id_invalido_no_explota() {
        let s = store();
        let v = get(&s, "/api/run/no-es-un-uuid");
        assert!(v["error"].is_string());
    }

    #[test]
    fn cancelar_deja_el_pedido_en_la_db() {
        // N-02: el pedido tiene que sobrevivir al proceso que lo hizo.
        let s = store();
        let id = s.create_run("ecommerce-etl", "p").unwrap();
        let v = post_json(&s, &format!("/api/run/{id}/cancel"), "");
        assert_eq!(v["ok"], true);
        assert!(s.cancel_requested(id), "queda persistido para el engine");
        assert_eq!(s.run_by_id(id).unwrap().unwrap().state, RunState::Cancelled);
        // Cancelar algo que ya termino avisa, no miente.
        let v2 = post_json(&s, &format!("/api/run/{id}/cancel"), "");
        assert_eq!(v2["ok"], false);
    }

    #[test]
    fn encolar_desde_la_ui() {
        let s = store();
        let v = post_json(
            &s,
            "/api/run",
            r#"{"dag":"ecommerce-etl","param":"2026-08-18"}"#,
        );
        assert_eq!(v["ok"], true);
        assert_eq!(s.queue_items().unwrap().len(), 1);
        // duplicado: rechaza con motivo
        let v2 = post_json(
            &s,
            "/api/run",
            r#"{"dag":"ecommerce-etl","param":"2026-08-18"}"#,
        );
        assert_eq!(v2["ok"], false);
        assert!(v2["motivo"].as_str().unwrap().contains("en cola"));
    }

    #[test]
    fn encolar_con_json_roto_no_tumba_el_server() {
        let s = store();
        let v = post_json(&s, "/api/run", "{esto no es json");
        assert_eq!(v["ok"], false);
        assert!(v["motivo"].as_str().unwrap().contains("json"));
    }

    #[test]
    fn queue_reporta_el_tope_configurado() {
        let s = store();
        crate::ops::set_queue_max(&s, 3).unwrap();
        s.enqueue("ecommerce-etl", "p", 0).unwrap();
        let v = get(&s, "/api/queue");
        assert_eq!(v["max_size"], 3);
        assert_eq!(v["items"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn graph_de_un_dag_desconocido_devuelve_vacio() {
        let s = store();
        let v = get(&s, "/api/graph?dag=no-existe");
        assert_eq!(v["nodes"].as_array().unwrap().len(), 0);
        // y el DAG real trae sus nodos
        let v = get(&s, "/api/graph?dag=ecommerce-etl");
        assert!(v["nodes"].as_array().unwrap().len() >= 6);
    }

    #[test]
    fn alerts_cuenta_los_fallos_del_dia() {
        let s = store();
        let id = s.create_run("ecommerce-etl", "malo").unwrap();
        s.set_run_state(id, RunState::Failed).unwrap();
        let v = get(&s, "/api/alerts");
        assert_eq!(v["fails_24h"].as_array().unwrap().len(), 1);
        assert_eq!(v["totales"], 1);
    }

    #[test]
    fn metrics_excluye_las_tareas_reutilizadas() {
        // N-07 visto desde la API que alimenta la tab Metricas.
        let s = store();
        let r1 = s.create_run("ecommerce-etl", "p").unwrap();
        s.set_task_state(TaskStateRecord::medido(
            r1,
            "validar",
            &TaskState::Success,
            1000,
            "ok",
            100,
            1.0,
        ))
        .unwrap();
        let r2 = s.create_run("ecommerce-etl", "p").unwrap();
        s.set_task_state(TaskStateRecord {
            reused: true,
            ..TaskStateRecord::medido(
                r2,
                "validar",
                &TaskState::Success,
                1000,
                "reutilizado",
                100,
                1.0,
            )
        })
        .unwrap();
        let v = get(&s, "/api/metrics");
        let t = &v["tareas"].as_array().unwrap()[0];
        assert_eq!(t["ejecuciones"], 1, "la reutilizada no infla el contador");
        assert_eq!(
            t["dag"], "ecommerce-etl",
            "la tarea se identifica con su DAG"
        );
    }

    /// El default del endpoint es HOY. Una corrida recien creada tiene que
    /// aparecer sin pasarle ningun filtro: el bug original era justamente que
    /// la tabla estaba vacia hasta que cerrara el mes.
    #[test]
    fn metrics_sin_filtros_muestra_lo_de_hoy() {
        let s = store();
        let r = s.create_run("ventas", "p").unwrap();
        s.set_run_state(r, zeruel_core::RunState::Success).unwrap();
        let v = get(&s, "/api/metrics");
        assert_eq!(
            v["volumen"].as_array().unwrap().len(),
            1,
            "la corrida de hoy tiene que estar: {v}"
        );
        assert_eq!(v["dags"].as_array().unwrap()[0]["dag"], "ventas");
    }

    /// Los filtros que se pidieron, por la query string.
    #[test]
    fn metrics_acepta_los_filtros_por_query() {
        let s = store();
        for (dag, mb) in [("liviano", 10u64), ("pesado", 900)] {
            let r = s.create_run(dag, "p").unwrap();
            s.set_task_state(TaskStateRecord::medido(
                r,
                "t",
                &TaskState::Success,
                0,
                "ok",
                mb,
                0.0,
            ))
            .unwrap();
            s.set_run_state(r, zeruel_core::RunState::Success).unwrap();
        }
        let v = get(&s, "/api/metrics?ram_min=500");
        let d = v["dags"].as_array().unwrap();
        assert_eq!(d.len(), 1, "solo el pesado: {v}");
        assert_eq!(d[0]["dag"], "pesado");

        let v = get(&s, "/api/metrics?dag=liviano");
        assert_eq!(v["dags"].as_array().unwrap()[0]["dag"], "liviano");
        assert_eq!(v["filtro"]["dag"], "liviano");
    }

    /// El boton de la cola. Un item que todavia no arranco no tiene UUID de
    /// run, asi que `/api/run/<id>/cancel` no lo alcanza: desde la cola es lo
    /// unico que se puede cancelar.
    #[test]
    fn se_cancela_un_item_de_la_cola_por_su_id() {
        let s = store();
        s.enqueue("d", "p", 0).unwrap();
        let id = s.queue_items().unwrap()[0].id;

        let v = post_json(&s, &format!("/api/cola/{id}/cancelar"), "");
        assert_eq!(v["ok"], true, "{v}");
        assert!(s.queue_items().unwrap().is_empty(), "sale de la cola");
    }

    #[test]
    fn cancelar_un_item_inexistente_no_dice_que_si() {
        let s = store();
        let v = post_json(&s, "/api/cola/4242/cancelar", "");
        assert_eq!(v["ok"], false, "{v}");
        assert!(v["error"].as_str().unwrap().contains("4242"), "{v}");
    }

    #[test]
    fn retry_sin_fallos_lo_dice() {
        let s = store();
        let v = post_json(&s, "/api/retry", "");
        assert_eq!(v["ok"], false);
        assert!(v["motivo"]
            .as_str()
            .unwrap()
            .contains("no hay runs fallidos"));
    }
}

#[cfg(test)]
mod tests_log {
    use super::*;

    #[test]
    fn saca_los_colores_pero_no_el_texto() {
        let crudo =
            "\u{1b}[2m2026-08-24T18:31:31Z\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m termino task=x";
        assert_eq!(sin_ansi(crudo), "2026-08-24T18:31:31Z  INFO termino task=x");
    }

    #[test]
    fn una_linea_sin_colores_queda_igual() {
        assert_eq!(sin_ansi("texto comun"), "texto comun");
        assert_eq!(sin_ansi(""), "");
    }

    #[test]
    fn una_secuencia_cortada_al_final_no_rompe() {
        // Al leer desde el medio del archivo una secuencia puede quedar partida.
        // No puede entrar en bucle ni entregar basura.
        assert_eq!(sin_ansi("hola \u{1b}[3"), "hola ");
    }
}

#[cfg(test)]
mod tests_ui {
    /// Las claves con las que se guardan los datos tienen que ser las mismas
    /// con las que se leen.
    ///
    /// Un barrido de traduccion renombro `runs` a `corridas` en la clave pero
    /// no en los `d.runs` que la leen. Quedo `undefined`, el `.filter` tiro
    /// excepcion, y la pantalla entera se fue en blanco mostrando "sin conexion
    /// con el servidor" — un mensaje que no tenia nada que ver con la causa y
    /// mandaba a buscar el problema en el servidor.
    ///
    /// El JavaScript no avisa de esto: leer una clave que no existe da
    /// `undefined` y explota recien al usarlo.
    #[test]
    fn las_claves_de_los_datos_coinciden_con_como_se_leen() {
        let ui = include_str!("ui.html");

        // Las que se guardan, del arreglo de pedidos. Puede haber varias
        // por renglon: ['dags', '/api/dags'], ['runs', '/api/runs'], ...
        let mut guardadas: Vec<&str> = Vec::new();
        for l in ui.lines().filter(|l| l.contains("'/api/")) {
            for trozo in l.split("['").skip(1) {
                if let Some((k, resto)) = trozo.split_once('\'') {
                    if resto.contains("'/api/") {
                        guardadas.push(k);
                    }
                }
            }
        }

        assert!(
            guardadas.len() >= 6,
            "esperaba encontrar las claves en el arreglo de pedidos, y saque: {guardadas:?}"
        );

        // Las que se leen: `datos.X` y `d.X` sobre el objeto de datos.
        for clave in &guardadas {
            let leida = format!("datos.{clave}");
            let corta = format!("d.{clave}");
            assert!(
                ui.contains(&leida) || ui.contains(&corta),
                "la clave `{clave}` se guarda pero nunca se lee: o sobra, o alguien                  renombro un lado solo"
            );
        }

        // Y al reves: lo que se lee tiene que guardarse.
        for clave in ["dags", "runs", "hyg", "col", "met", "al"] {
            assert!(
                guardadas.contains(&clave),
                "la interfaz lee `{clave}` pero no esta entre las que se piden: {guardadas:?}"
            );
        }
    }
}
