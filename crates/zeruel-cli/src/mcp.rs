//! MCP server por stdio (JSON-RPC 2.0): expone zeruel a herramientas de IA.
//!
//! | Tool | Para que |
//! |---|---|
//! | `list_dags` | Que pipelines hay: desplegados como binario y compilados |
//! | `trigger_run` | Encolar un run |
//! | `run_status` / `task_metrics` | Como fue un run, tarea por tarea |
//! | `recursos` | RAM libre, runs en curso, cola: si conviene lanzar algo ahora |
//! | `memoria_por_tarea` | Pico historico por tarea + techo sugerido para --ram-max |
//! | `gc_status` | Higiene: tamaño de la base, ultima limpieza |
//! | `queue` / `queue_set_max` | Ver y acotar la cola |
//! | `backfill` | Un run por dia en un rango |
//! | `sla` | Tope de espera en cola |
//!
//! Las de lectura (`recursos`, `memoria_por_tarea`) existen para que una IA
//! pueda decidir *antes* de actuar, en vez de encolar a ciegas y descubrir que
//! no habia memoria.

use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use serde_json::{json, Value};
use zeruel_core::Store;

pub fn run(state: PathBuf) -> Result<()> {
    let store = Arc::new(Store::open(&state)?);
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();

    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let req: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
        let params = req.get("params").cloned().unwrap_or(json!({}));

        // Sin `id` es una NOTIFICACION: se procesa y no se responde. Antes se
        // contestaba a todo, asi que el `notifications/initialized` que manda
        // cualquier cliente MCP apenas termina el handshake recibia un
        // `{"id":null,"error":-32601}` que el spec prohibe (N-42).
        let Some(id) = req.get("id").cloned() else {
            continue;
        };

        let resp = match method {
            "initialize" => {
                json!({"protocolVersion": "2024-11-05", "capabilities": {"tools": {}}, "serverInfo": {"name": "zeruel", "version": env!("CARGO_PKG_VERSION")}})
            }
            "ping" => json!({}),
            "tools/list" => tools_list(),
            "tools/call" => handle_tool_call(&store, &params),
            _ => {
                json!({"error": {"code": -32601, "message": format!("metodo desconocido: {method}")}})
            }
        };

        let out = if let Some(err) = resp.get("error") {
            json!({"jsonrpc": "2.0", "id": id, "error": err})
        } else {
            json!({"jsonrpc": "2.0", "id": id, "result": resp})
        };

        let mut lock = stdout.lock();
        writeln!(lock, "{}", out)?;
        lock.flush()?;
    }
    Ok(())
}

fn tools_list() -> Value {
    json!({"tools": [
        {
            "name": "list_dags",
            "description": "Lista los pipelines: los binarios desplegados en el directorio de pipelines (los que realmente corre el scheduler) y los compilados en el binario.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "trigger_run",
            "description": "Encola un run para el scheduler",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "dag": {"type": "string", "description": "Nombre del DAG"},
                    "param": {"type": "string", "description": "Parametro del run"},
                    "priority": {"type": "integer", "default": 0}
                },
                "required": ["dag", "param"]
            }
        },
        {
            "name": "run_status",
            "description": "Estado de un run por UUID o 'latest'",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "run": {"type": "string", "default": "latest"}
                }
            }
        },
        {
            "name": "task_metrics",
            "description": "Metrics por tarea de un run (rows, ram, cpu, estado)",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "run": {"type": "string", "default": "latest"}
                }
            }
        },
        {
            "name": "recursos",
            "description": "Que recursos hay disponibles ahora: RAM libre, runs en curso, cola y tamaño de la base. Sirve para decidir si conviene lanzar algo.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "memoria_por_tarea",
            "description": "Pico historico de RAM y CPU por tarea, con un techo de memoria sugerido para `--ram-max`.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "gc_status",
            "description": "Pulso de higiene del sistema (db size, runs, ultima GC)",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "queue",
            "description": "Items en cola del scheduler",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "backfill",
            "description": "Encola un run por dia en un rango de fechas (YYYY-MM-DD)",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "dag": {"type": "string", "description": "Nombre del DAG"},
                    "desde": {"type": "string", "description": "Fecha inicial YYYY-MM-DD"},
                    "hasta": {"type": "string", "description": "Fecha final YYYY-MM-DD"},
                    "priority": {"type": "integer", "default": 0}
                },
                "required": ["dag", "desde", "hasta"]
            }
        },
        {
            "name": "sla",
            "description": "Fija el maximo de espera (segundos) de un DAG en cola. max_wait=0 borra el tope",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "dag": {"type": "string", "description": "Nombre del DAG"},
                    "max_wait": {"type": "integer", "description": "Segundos maximo de espera (0 = borrar tope)"}
                },
                "required": ["dag", "max_wait"]
            }
        },
        {
            "name": "queue_set_max",
            "description": "Fijar el tope global de items en cola (cola acotada). 0 = sin tope",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "max": {"type": "integer", "description": "Maximo de items en cola (0 = sin tope)"}
                },
                "required": ["max"]
            }
        }
    ]})
}

/// Busca un run por "latest" o por UUID.
fn buscar_run(store: &Store, run: &str) -> Option<zeruel_core::Run> {
    if run == "latest" {
        return store.recent_runs(1, 0).ok()?.into_iter().next();
    }
    // Por id directo contra la DB: antes se traian los 200 runs mas recientes
    // y se buscaba con un `find`, asi que un run mas viejo "no existia" (N-13).
    let id = uuid::Uuid::parse_str(run).ok()?;
    store.run_by_id(id).ok().flatten()
}

fn handle_tool_call(store: &Store, params: &Value) -> Value {
    let name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
    let args = params.get("arguments").cloned().unwrap_or(json!({}));
    let mut es_error = false;
    let result = match name {
        // Sale del registry (N-04): estaba hardcodeado a ["ecommerce-etl"], asi
        // que cualquier DAG nuevo era invisible para las IAs. Ahora ademas
        // incluye los binarios desplegados, que son los que realmente corren.
        "list_dags" => {
            let mut reg = crate::pipelines::Registro::new();
            let dir = std::env::var_os("ZERUEL_PIPELINES")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| std::path::PathBuf::from("pipelines"));
            let desplegados: Vec<_> = reg
                .escanear(&dir)
                .into_iter()
                .map(|p| {
                    json!({
                        "dag": p.desc.name,
                        "origen": "binario desplegado",
                        "binario": p.bin.to_string_lossy(),
                        "cron": p.desc.schedule,
                        "tareas": p.desc.tasks.len(),
                    })
                })
                .collect();
            let compilados: Vec<_> = crate::dags::names()
                .into_iter()
                .filter(|n| !desplegados.iter().any(|d| d["dag"] == *n))
                .map(|n| json!({"dag": n, "origen": "compilado en el binario"}))
                .collect();
            json!({
                "desplegados": desplegados,
                "compilados": compilados,
                "directorio_pipelines": dir.to_string_lossy(),
            })
        }

        // Que consume zeruel. Es la pregunta que una IA necesita para decidir si
        // conviene lanzar algo ahora.
        "recursos" => {
            let h = store.hygiene().unwrap_or_default();
            let mut sys = sysinfo::System::new_all();
            sys.refresh_memory();
            let libre_mb = sys.available_memory() / (1024 * 1024);
            let total_mb = sys.total_memory() / (1024 * 1024);
            let corriendo = store.running_keys().map(|k| k.len()).unwrap_or(0);
            json!({
                "ram_libre_mb": libre_mb,
                "ram_total_mb": total_mb,
                "runs_en_curso": corriendo,
                "items_en_cola": store.queue_items().map(|q| q.len()).unwrap_or(0),
                "db_mb": (h.db_size_bytes as f64 / 1_048_576.0 * 10.0).round() / 10.0,
                "db_grande": h.db_mb_warning,
                "nota": "el consumo en reposo de serve+web es ~49 MB; ver docs/COSTOS.md. Para el desglose por proceso y el estado en disco, GET /api/salud o la pestaña Sistema de la UI.",
            })
        }

        // El pico historico por tarea: sirve para elegir un techo de memoria.
        "memoria_por_tarea" => {
            let stats = store.task_stats().unwrap_or_default();
            let pico = stats.iter().map(|t| t.ram_peak).max().unwrap_or(0);
            json!({
                "tareas": stats.iter().map(|t| json!({
                    "tarea": t.task,
                    "ram_pico_mb": t.ram_peak,
                    "filas_prom": t.filas_prom.round(),
                    "cpu_prom_seg": (t.cpu_prom * 100.0).round() / 100.0,
                    "ejecuciones": t.ejecuciones,
                })).collect::<Vec<_>>(),
                "pico_maximo_mb": pico,
                "techo_sugerido_mb": if pico > 0 { pico + pico / 4 } else { 0 },
                "nota": "el techo sugerido es el pico historico + 25%. Se fija con `zeruel serve --ram-max`.",
            })
        }
        "trigger_run" => {
            let dag = args.get("dag").and_then(|d| d.as_str()).unwrap_or("");
            let param = args.get("param").and_then(|d| d.as_str()).unwrap_or("");
            let prio = args.get("priority").and_then(|d| d.as_i64()).unwrap_or(0);
            let r = store
                .enqueue(dag, param, prio)
                .unwrap_or(zeruel_core::EnqueueResult {
                    ok: false,
                    motivo: "error interno".into(),
                });
            json!({"ok": r.ok, "motivo": r.motivo})
        }
        "run_status" => {
            let run = args.get("run").and_then(|d| d.as_str()).unwrap_or("latest");
            match buscar_run(store, run) {
                Some(r) => {
                    json!({"id": r.id.to_string(), "dag": r.dag, "param": r.param, "state": format!("{:?}", r.state), "started_at": r.started_at, "finished_at": r.finished_at})
                }
                None => {
                    es_error = true;
                    json!({"error": format!("run no encontrado: {run}")})
                }
            }
        }
        "task_metrics" => {
            let run = args.get("run").and_then(|d| d.as_str()).unwrap_or("latest");
            match buscar_run(store, run) {
                Some(r) => {
                    let tasks = store.task_runs(r.id).unwrap_or_default();
                    json!({
                        "run_id": r.id.to_string(),
                        "state": format!("{:?}", r.state),
                        "tasks": tasks.iter().map(|t| json!({
                            "task": t.task,
                            "state": format!("{:?}", t.state),
                            "rows": t.rows,
                            "ram_peak_mb": t.ram_peak_mb,
                            "cpu_secs": t.cpu_secs
                        })).collect::<Vec<_>>()
                    })
                }
                None => json!({"error": "sin runs"}),
            }
        }
        "gc_status" => {
            let h = store.hygiene().unwrap_or_default();
            json!({"db_bytes": h.db_size_bytes, "runs": h.runs, "task_runs": h.task_runs, "meses": h.meses, "ultima_gc": h.ultima_gc, "db_mb_warning": h.db_mb_warning})
        }
        "queue" => {
            let q = store.queue_items().unwrap_or_default();
            let max = store
                .meta_get("queue.max_size")
                .ok()
                .flatten()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(0);
            json!({"items": q.iter().map(|i| json!({"id": i.id, "dag": i.dag, "param": i.param, "priority": i.priority, "state": i.state})).collect::<Vec<_>>(), "max_size": max, "total": q.len()})
        }
        // Backfill y SLA usan las MISMAS funciones que la CLI (`ops.rs`): eran
        // dos implementaciones distintas y la de aca se salteaba el ultimo dia
        // del rango (N-05) y le daba otro significado a max_wait=0 (N-12).
        "backfill" => {
            let dag = args.get("dag").and_then(|d| d.as_str()).unwrap_or("");
            let desde = args.get("desde").and_then(|d| d.as_str()).unwrap_or("");
            let hasta = args.get("hasta").and_then(|d| d.as_str()).unwrap_or("");
            let prio = args.get("priority").and_then(|d| d.as_i64()).unwrap_or(0);
            match crate::ops::backfill(store, dag, desde, hasta, prio) {
                Ok(r) => json!({
                    "ok": true, "dag": r.dag, "desde": r.desde, "hasta": r.hasta,
                    "encolados": r.encolados, "totales": r.totales, "skipped": r.skipped,
                }),
                Err(e) => {
                    es_error = true;
                    json!({"ok": false, "error": format!("{e:#}")})
                }
            }
        }
        "sla" => {
            let dag = args.get("dag").and_then(|d| d.as_str()).unwrap_or("");
            let max_wait = args.get("max_wait").and_then(|d| d.as_i64()).unwrap_or(-1);
            match crate::ops::set_sla(store, dag, max_wait) {
                Ok(Some(n)) => json!({"ok": true, "dag": dag, "max_wait": n}),
                Ok(None) => json!({"ok": true, "dag": dag, "max_wait": 0, "msg": "tope borrado"}),
                Err(e) => {
                    es_error = true;
                    json!({"ok": false, "error": format!("{e:#}")})
                }
            }
        }
        "queue_set_max" => {
            let max = args.get("max").and_then(|d| d.as_u64()).unwrap_or(0) as usize;
            let current = store.queue_items().map(|q| q.len()).unwrap_or(0);
            let msg = if max == 0 {
                "tope quitado".to_string()
            } else {
                format!("tope fijado a {max} items")
            };
            match crate::ops::set_queue_max(store, max) {
                Ok(()) => json!({"ok": true, "max_size": max, "current": current, "msg": msg}),
                Err(e) => {
                    es_error = true;
                    json!({"ok": false, "error": format!("{e:#}")})
                }
            }
        }
        _ => {
            es_error = true;
            json!({"error": format!("tool desconocida: {name}")})
        }
    };
    // `isError` es como el spec de MCP marca que la tool fallo: sin esto el
    // cliente ve un resultado exitoso con un texto de error adentro (N-42).
    json!({
        "content": [{"type": "text", "text": result.to_string()}],
        "isError": es_error,
    })
}
