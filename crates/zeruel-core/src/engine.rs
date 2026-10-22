//! Engine de ejecucion de runs: orden topologico, worker pool,
//! checkpoints, retries con backoff y re-levantado (durable execution).
//!
//! Modelo: cada task es un `Arc<dyn Task>` registrado por `TaskId`.
//! El engine itera hasta que no queda trabajo: lanza las tareas cuyas
//! deps terminaron (hasta `max_parallel` a la vez), recoge resultados
//! y checkpointea cada estado en el store. Si el proceso muere, el
//! proximo `run` sobre el mismo DAG+param retoma donde estaba.
//!
//! Retries: si una tarea falla, el engine la reintenta con backoff
//! exponencial (2^i * base) hasta `Dag.max_retries`. Solo cuando se
//! agotan los intentos la marca definitivamente Failed y el run falla.
//!
//! Wasm (B.1): una tarea puede declararse en el DAG con
//! `TaskDef.wasm = Some(WasmSpec { path, fuel, mem_mb })`. El engine
//! la compila UNA vez por run (reutilizando `WasmTask::new`) y la
//! despacha como una tarea mas del grafo (deps/param/rows/retries
//! genericos). El caller no la registra en el hashmap de tareas.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use anyhow::Result;
use tracing::{info, warn};

use zeruel_sdk::{Dag, Task, TaskContext, TaskDef, TaskId, TaskState};

use crate::store::{RunState, Store, TaskStateRecord};

/// Vigila la DB por si otro proceso pide cancelar este run.
///
/// La señal de cancelacion es un `AtomicBool` en memoria: alcanza cuando quien
/// cancela y quien ejecuta son el mismo proceso, pero el caso real es otro
/// (N-02) — la UI (`zeruel web`) cancela un run que corre en `zeruel serve`. El
/// watcher consulta `cancel:<run_id>` en `meta` cada 250ms y, si aparece,
/// prende el flag local: de ahi en mas funciona todo lo que ya estaba (el
/// engine no lanza mas tasks y el sandbox wasm aborta en vivo por epoch).
struct CancelWatcher {
    fin: Arc<std::sync::atomic::AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl CancelWatcher {
    fn start(
        store: Arc<Store>,
        run_id: uuid::Uuid,
        cancel: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        let fin = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let fin_t = Arc::clone(&fin);
        let handle = std::thread::Builder::new()
            .name("zeruel-cancel-watch".into())
            .spawn(move || {
                use std::sync::atomic::Ordering as O;
                while !fin_t.load(O::Relaxed) {
                    if cancel.load(O::SeqCst) {
                        return; // ya esta cancelado, no hace falta seguir mirando
                    }
                    if store.cancel_requested(run_id) {
                        warn!(run = %run_id, "cancelacion pedida desde otro proceso");
                        cancel.store(true, O::SeqCst);
                        return;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(250));
                }
            })
            .ok();
        Self { fin, handle }
    }
}

impl Drop for CancelWatcher {
    fn drop(&mut self) {
        self.fin.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

pub struct Engine {
    pub store: Arc<Store>,
    /// Base del backoff exponencial entre reintentos (2^i * base). Default 2000ms.
    retry_base_ms: u64,
    /// Techo de RAM del proceso, en MB. `None` = sin techo.
    techo_mb: Option<u64>,
}

/// Resultado de ejecutar (o retomar) un run.
#[derive(Debug)]
pub struct RunReport {
    pub run_id: uuid::Uuid,
    pub success: bool,
    pub total_rows: u64,
    pub skipped: usize,  // tareas reutilizadas de un run anterior (durabilidad)
    pub cancelled: bool, // se corto por cancelacion desde UI/API
    /// Se corto por pasarse del techo de memoria. Es un fallo, no una
    /// cancelacion: se distingue para no mandar a buscar la causa equivocada.
    pub paso_el_techo: bool,
}

type SharedState = StdMutex<HashMap<TaskId, TaskState>>;

/// Todo lo que necesita un intento de tarea (deuda B-01: eran 8 argumentos
/// sueltos, con dos `Arc` y dos `&Arc` seguidos que se podian cruzar sin que el
/// compilador dijera nada).
struct RetryCtx {
    id: TaskId,
    run_id: uuid::Uuid,
    max_retries: u32,
    base_ms: u64,
    /// Techo de RAM del proceso en MB (`None` = sin techo).
    techo_mb: Option<u64>,
    /// Se prende si el run se paso del techo (compartida por todas las tareas).
    paso_el_techo: Arc<std::sync::atomic::AtomicBool>,
    task: Arc<dyn Task>,
    ctx: Arc<TaskContext>,
    store: Arc<Store>,
    shared: Arc<SharedState>,
}

impl Engine {
    pub fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            retry_base_ms: 2000,
            techo_mb: None,
        }
    }

    /// Base del backoff entre reintentos (ms). Se usa 2^i * base entre el
    /// intento i y el i+1. Baja en tests para no esperar.
    pub fn with_retry_base_ms(mut self, ms: u64) -> Self {
        self.retry_base_ms = ms;
        self
    }

    /// Techo de RAM para el proceso, en MB.
    ///
    /// Aplica **dos topes a la vez**, que se complementan:
    ///
    /// 1. El **blando** en `mb`: el sampler mira el RSS cada 25 ms y corta el
    ///    run con un error que dice qué tarea fue, cuánto usó y cuál era el
    ///    límite.
    /// 2. El **duro** un poco más arriba ([`crate::limite::con_margen`]),
    ///    impuesto por el sistema operativo. Ataja la reserva repentina que al
    ///    blando se le escapa entre dos muestras, al precio de que el proceso
    ///    muere sin poder explicar nada.
    ///
    /// Por eso el blando va primero: en el caso normal salta él y queda el
    /// diagnóstico. El duro es la red.
    ///
    /// El duro solo está implementado en Windows; donde no, queda el blando
    /// y se sigue igual.
    pub fn con_techo_mb(mut self, mb: u64) -> Self {
        self.techo_mb = if mb == 0 { None } else { Some(mb) };
        if let Some(m) = self.techo_mb {
            crate::limite::aplicar(crate::limite::con_margen(m));
        }
        self
    }

    /// Resuelve las tareas que el DAG declara con un motor propio (wasm o sql)
    /// y las suma al hashmap que paso el caller. Cada `WasmTask` se compila UNA
    /// vez por run (wasmtime es caro; se amortiza en todos los intentos).
    ///
    /// Si el DAG pide un motor que este binario no compilo, el run **falla acá**
    /// con un mensaje que dice qué feature falta. Ignorarlas en silencio dejaría
    /// un DAG a medio ejecutar sin que nadie se entere.
    fn effective_tasks(
        dag: &Dag,
        tasks: &HashMap<TaskId, Arc<dyn Task>>,
    ) -> Result<HashMap<TaskId, Arc<dyn Task>>> {
        // El `mut` solo se usa si hay algun motor compilado; sin features, este
        // mapa se devuelve tal cual.
        #[cfg_attr(not(any(feature = "wasm", feature = "sql")), allow(unused_mut))]
        let mut eff: HashMap<TaskId, Arc<dyn Task>> = tasks.clone();

        for def in &dag.tasks {
            // El caller ya registro una implementacion con ese id: no se pisa.
            if eff.contains_key(&def.id) {
                continue;
            }

            if let Some(_spec) = &def.wasm {
                #[cfg(feature = "wasm")]
                {
                    let t: Arc<dyn Task> = crate::wasm::WasmTask::new(
                        &def.id.0,
                        &_spec.path,
                        _spec.fuel,
                        _spec.mem_mb,
                    )?;
                    eff.insert(def.id.clone(), t);
                }
                #[cfg(not(feature = "wasm"))]
                anyhow::bail!(
                    "la tarea '{}' del DAG '{}' es WASM, pero este binario se compilo sin el \
                     feature `wasm`. Agregalo en Cargo.toml (zeruel = {{ features = [\"wasm\"] }}) \
                     o registra una implementacion propia para esa tarea.",
                    def.id.0,
                    dag.name
                );
            } else if let Some(_spec) = &def.sql {
                #[cfg(feature = "sql")]
                {
                    let t: Arc<dyn Task> =
                        Arc::new(crate::sql::SqlTask::new(&def.id.0, _spec.clone()));
                    eff.insert(def.id.clone(), t);
                }
                #[cfg(not(feature = "sql"))]
                anyhow::bail!(
                    "la tarea '{}' del DAG '{}' es SQL, pero este binario se compilo sin el \
                     feature `sql`. Agregalo en Cargo.toml (zeruel = {{ features = [\"sql\"] }}) \
                     o registra una implementacion propia para esa tarea.",
                    def.id.0,
                    dag.name
                );
            }
        }

        Ok(eff)
    }

    /// Ejecuta (o retoma) un run del DAG.
    pub fn run(
        &self,
        dag: &Dag,
        tasks: &HashMap<TaskId, Arc<dyn Task>>,
        param: &str,
        max_parallel: usize,
    ) -> Result<RunReport> {
        self.run_con_id(dag, tasks, param, max_parallel, None)
    }

    /// Igual que [`Engine::run`], pero pudiendo **adoptar un run ya creado**.
    ///
    /// Lo usa el scheduler multiproceso: crea la fila del run —asi tiene el id
    /// desde el momento cero y puede cancelarlo— y se lo pasa al subproceso del
    /// pipeline por `--run-id`. Sin esto, el scheduler tendria que esperar a que
    /// el subproceso le informe su id, y en esa ventana no habria forma de
    /// cancelarlo.
    pub fn run_con_id(
        &self,
        dag: &Dag,
        tasks: &HashMap<TaskId, Arc<dyn Task>>,
        param: &str,
        max_parallel: usize,
        run_id_externo: Option<uuid::Uuid>,
    ) -> Result<RunReport> {
        let dag_name = dag.name.as_str();

        // 0a. resolver las tareas wasm declaradas en el DAG
        let tasks = Self::effective_tasks(dag, tasks)?;
        let tasks = Arc::new(tasks);

        // 0b. sanity: todo el DAG debe tener tarea registrada (fallar rapido, no colgarse)
        for t in &dag.tasks {
            if !tasks.contains_key(&t.id) {
                anyhow::bail!(
                    "task '{}' del DAG '{}' no tiene implementacion registrada",
                    t.id.0,
                    dag_name
                );
            }
        }

        // 1. run: el que nos dieron, el que quedo a medias (mal kill), o uno nuevo.
        //    La busqueda es por dag+param: retomar por dag solo mezclaba los
        //    checkpoints de un param con el run de otro (N-06).
        let run_id = match run_id_externo {
            Some(id) => {
                info!(run = %id, dag = dag_name, param, "corriendo el run que asigno el scheduler");
                id
            }
            None => match self.store.recover_running(dag_name, param)? {
                Some(id) => {
                    warn!(run = %id, dag = dag_name, param, "retomando run en progreso");
                    id
                }
                None => self.store.create_run(dag_name, param)?,
            },
        };
        self.store.set_run_state(run_id, RunState::Running)?;

        // 2. estado inicial por tarea (durabilidad: los Success previos se saltan)
        let state: Arc<SharedState> = Arc::new(StdMutex::new(HashMap::new()));
        let mut skipped = 0usize;
        for id in tasks.keys() {
            let s = match self.store.task_initial_state(dag_name, param, &id.0)? {
                TaskState::Success => {
                    skipped += 1;
                    info!(task = %id.0, "skip: ya exitoso en run previo");
                    // Se copian las metricas del run donde SI se ejecuto y la
                    // fila queda marcada `reused` (N-07): antes se escribian
                    // ceros, que despues hundian los promedios de la tab
                    // Metricas y hacian que un re-run reportara `filas: 0`.
                    let (rows, ram, cpu) = self
                        .store
                        .last_success_metrics(dag_name, param, &id.0)
                        .unwrap_or((0, 0, 0.0));
                    self.store.set_task_state(TaskStateRecord {
                        reused: true,
                        ..TaskStateRecord::medido(
                            run_id,
                            &id.0,
                            &TaskState::Success,
                            rows,
                            "reutilizado de un run anterior",
                            ram,
                            cpu,
                        )
                    })?;
                    TaskState::Success
                }
                other => other,
            };
            state.lock().unwrap().insert(id.clone(), s);
        }

        let order = dag.topological_order()?;
        let cancel = self.store.cancel_flag(run_id);
        // Si otro proceso ya habia pedido cancelar este run, arrancamos con la
        // señal puesta (no esperamos al primer tick del watcher).
        if self.store.cancel_requested(run_id) {
            cancel.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        let _watcher = CancelWatcher::start(Arc::clone(&self.store), run_id, Arc::clone(&cancel));
        // Compartida por todas las tareas: la prende el sampler si el proceso
        // se pasa del techo de memoria.
        let techo_excedido = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ctx = Arc::new(TaskContext {
            run_id,
            param: param.to_string(),
            cancel: Some(Arc::clone(&cancel)),
        });
        let max_parallel = max_parallel.max(1);

        let mut total_rows = 0u64;
        let mut failed: Option<(String, String)> = None;

        // 3. bucle principal: lanzar listas, recoger terminados
        loop {
            if failed.is_some() {
                break;
            }
            if cancel.load(std::sync::atomic::Ordering::SeqCst) {
                warn!(run = %run_id, "cancelacion: no se lanzan mas tasks");
                break;
            }

            // tareas listas: Pending sin deps pendientes y hay cupo
            let ready: Vec<TaskId> = {
                let st = state.lock().unwrap();
                let in_flight = st
                    .values()
                    .filter(|s| matches!(s, TaskState::Running))
                    .count();
                st.iter()
                    .filter(|(id, s)| {
                        matches!(s, TaskState::Pending)
                            && ready_slot(id, &st, &order)
                            && in_flight < max_parallel
                    })
                    .map(|(id, _)| id.clone())
                    .collect()
            };

            if ready.is_empty() {
                // sin trabajo nuevo: seguir si hay en vuelo, si no terminar
                let st = state.lock().unwrap();
                let in_flight = st
                    .values()
                    .filter(|s| matches!(s, TaskState::Running))
                    .count();
                if in_flight == 0 {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(25));
                continue;
            }

            let mut handles = Vec::new();
            for id in ready {
                // tomar cupo bajo lock para que no lo tome otro thread
                let taken = {
                    let mut st = state.lock().unwrap();
                    let in_flight = st
                        .values()
                        .filter(|s| matches!(s, TaskState::Running))
                        .count();
                    if !matches!(st.get(&id), Some(TaskState::Pending)) || in_flight >= max_parallel
                    {
                        false
                    } else {
                        st.insert(id.clone(), TaskState::Running);
                        true
                    }
                };
                if !taken {
                    continue;
                }
                self.store.set_task_state(TaskStateRecord::transicion(
                    run_id,
                    &id.0,
                    &TaskState::Running,
                    "iniciando",
                ))?;
                let task = tasks.get(&id).unwrap().clone();
                let store = Arc::clone(&self.store);
                let ctx = Arc::clone(&ctx);
                let st_state = Arc::clone(&state);
                let max_retries = dag.max_retries;
                let base_ms = self.retry_base_ms;
                let techo_mb = self.techo_mb;
                let paso_el_techo = Arc::clone(&techo_excedido);
                handles.push((
                    id.clone(),
                    std::thread::spawn(move || {
                        Self::ejecutar_con_retries(RetryCtx {
                            id,
                            run_id,
                            max_retries,
                            base_ms,
                            techo_mb,
                            paso_el_techo: Arc::clone(&paso_el_techo),
                            task,
                            ctx,
                            store,
                            shared: st_state,
                        })
                    }),
                ));
            }

            // recoger
            for (id, handle) in handles {
                let (rows, msg, ok) = handle
                    .join()
                    .map_err(|_| anyhow::anyhow!("task panico: {}", id.0))?;
                if ok {
                    total_rows += rows;
                } else {
                    failed = Some((id.0, msg));
                    break;
                }
            }
        }

        let final_ok = failed.is_none();
        if let Some((t, m)) = &failed {
            warn!(task = t, err = m, "run fallido");
        }
        let paso_techo = techo_excedido.load(std::sync::atomic::Ordering::SeqCst);
        let cancelled = cancel.load(std::sync::atomic::Ordering::SeqCst);
        // Pasarse del techo NO es una cancelacion: es un fallo del run. Se
        // distinguen porque el sampler usa la misma señal para cortar, y
        // reportarlo como "cancelado" mandaria a buscar a quien lo cancelo.
        let st = if paso_techo {
            RunState::Failed
        } else if cancelled {
            RunState::Cancelled
        } else if final_ok {
            RunState::Success
        } else {
            RunState::Failed
        };
        if paso_techo {
            warn!(
                run = %run_id,
                techo_mb = self.techo_mb.unwrap_or(0),
                "el run se corto por pasarse del techo de memoria"
            );
        }
        self.store.set_run_state(run_id, st)?;
        Ok(RunReport {
            run_id,
            success: final_ok && !cancelled && !paso_techo,
            total_rows,
            skipped,
            cancelled: cancelled && !paso_techo,
            paso_el_techo: paso_techo,
        })
    }

    /// Ejecuta una tarea con reintentos y backoff exponencial.
    /// Devuelve (rows, mensaje, ok).
    fn ejecutar_con_retries(rc: RetryCtx) -> (u64, String, bool) {
        let RetryCtx {
            id,
            run_id,
            max_retries,
            base_ms,
            techo_mb,
            paso_el_techo,
            task,
            ctx,
            store,
            shared,
        } = rc;
        let store = &store;
        let shared = &shared;
        let mut msg = String::new();
        for intento in 0..=max_retries {
            // Si el run fue cancelado, no (re)intentar: sale como no-ok.
            // La señal es la misma para una cancelacion manual y para el techo
            // de memoria, asi que se mira la bandera para decir cual fue.
            if let Some(c) = &ctx.cancel {
                if c.load(std::sync::atomic::Ordering::SeqCst) {
                    msg = if paso_el_techo.load(std::sync::atomic::Ordering::SeqCst) {
                        format!(
                            "el run se paso del techo de {} MB de memoria",
                            techo_mb.unwrap_or(0)
                        )
                    } else {
                        "run cancelado".to_string()
                    };
                    break;
                }
            }
            if intento > 0 {
                let backoff_ms = base_ms.saturating_mul(1u64 << intento.min(10)).max(base_ms);
                info!(task = %id.0, intento, backoff_ms, "reintento tras backoff");
                std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
                let _ = store.set_task_state(TaskStateRecord::transicion(
                    run_id,
                    &id.0,
                    &TaskState::Running,
                    "reintento",
                ));
                shared
                    .lock()
                    .unwrap()
                    .insert(id.clone(), TaskState::Running);
            }
            let t0 = std::time::Instant::now();
            info!(task = %id.0, intento, "ejecutando");
            let sampler = crate::metrics::Sampler::con_techo_y_aviso(
                techo_mb,
                ctx.cancel.clone(),
                Some(Arc::clone(&paso_el_techo)),
            );
            let result = task.run(&ctx);
            let excedio = sampler.excedio_el_techo();
            let m = sampler.stop();
            info!(
                task = %id.0, intento,
                elapsed = ?t0.elapsed(),
                ram_peak_mb = m.peak_ram_mb,
                cpu_secs = format!("{:.0}", m.cpu_secs),
                "termino"
            );
            let ok = result.is_ok();
            let rows = result.as_ref().map(|o| o.rows).unwrap_or(0);
            msg = match &result {
                Ok(out) => out.message.clone(),
                // Si se paso del techo, el error que ve la tarea es "cancelado":
                // el sampler prende esa misma señal. Se aclara la causa real,
                // porque "cancelado" mandaria a buscar a quien lo cancelo.
                Err(e) if excedio => format!(
                    "supero el techo de memoria de {} MB (pico {} MB): {e:#}",
                    techo_mb.unwrap_or(0),
                    m.peak_ram_mb
                ),
                Err(e) => format!("{e:#}"),
            };
            if excedio && ok {
                // Raro pero posible: la tarea termino bien justo despues de
                // pasarse. El pico igual quedo registrado y hay que avisar.
                warn!(
                    task = %id.0,
                    ram_peak_mb = m.peak_ram_mb,
                    "termino bien pero paso el techo de memoria"
                );
            }
            if ok {
                let s = TaskState::Success;
                // `intento` es 0-based: salir en la primera vuelta es 1 intento.
                let _ = store.set_task_state(TaskStateRecord::con_intentos(
                    run_id,
                    &id.0,
                    &s,
                    rows,
                    &msg,
                    m.peak_ram_mb,
                    m.cpu_secs,
                    intento + 1,
                ));
                shared.lock().unwrap().insert(id, s);
                return (rows, msg, true);
            }
            // fallo: checkpoint con reintentos restantes; el ultimo error queda en `msg`
            let s = TaskState::Failed {
                retries_left: max_retries.saturating_sub(intento + 1),
            };
            let _ = store.set_task_state(TaskStateRecord::con_intentos(
                run_id,
                &id.0,
                &s,
                0,
                &msg,
                m.peak_ram_mb,
                m.cpu_secs,
                intento + 1,
            ));
            shared.lock().unwrap().insert(id.clone(), s);
        }
        let s = TaskState::Failed { retries_left: 0 };
        let _ = store.set_task_state(TaskStateRecord::medido(run_id, &id.0, &s, 0, &msg, 0, 0.0));
        shared.lock().unwrap().insert(id, s);
        (0, msg, false)
    }
}

/// ¿La tarea `id` (en `st`) tiene todas sus deps en Success?
fn ready_slot(id: &TaskId, st: &HashMap<TaskId, TaskState>, order: &[&TaskDef]) -> bool {
    let Some(def) = order.iter().find(|t| &t.id == id) else {
        return false;
    };
    def.deps
        .iter()
        .all(|d| matches!(st.get(d), Some(TaskState::Success)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use zeruel_sdk::{TaskDef, TaskOutput};
    // Los motores opcionales solo se testean si se compilaron.
    #[cfg(feature = "sql")]
    use polars::{df, prelude as pl, prelude::IntoLazy};
    use std::sync::atomic::{AtomicU32, Ordering};
    #[cfg(feature = "sql")]
    use zeruel_sdk::SqlSpec;
    #[cfg(feature = "wasm")]
    use zeruel_sdk::WasmSpec;

    fn id(s: &str) -> TaskId {
        TaskId(s.into())
    }

    #[cfg(feature = "sql")]
    fn pl_path(p: &std::path::Path) -> pl::PlRefPath {
        p.to_string_lossy().as_ref().into()
    }
    #[cfg(feature = "sql")]
    fn write_parquet(df: &pl::DataFrame, p: &std::path::Path) {
        df.clone()
            .lazy()
            .sink(
                pl::SinkDestination::File {
                    target: pl::SinkTarget::Path(pl_path(p)),
                },
                pl::FileWriteFormat::Parquet(std::sync::Arc::new(
                    pl::ParquetWriteOptions::default(),
                )),
                pl::UnifiedSinkArgs::default(),
            )
            .unwrap()
            .collect()
            .unwrap();
    }
    #[cfg(feature = "sql")]
    fn read_parquet(p: &std::path::Path) -> pl::DataFrame {
        pl::LazyFrame::scan_parquet(pl_path(p), pl::ScanArgsParquet::default())
            .unwrap()
            .collect()
            .unwrap()
    }

    /// Tarea que falla los primeros `fallo_n` intentos y pasa despues.
    struct Flaky {
        n: AtomicU32,
        fallo_n: u32,
    }
    impl Task for Flaky {
        fn id(&self) -> TaskId {
            id("flaky")
        }
        fn run(&self, _ctx: &TaskContext) -> anyhow::Result<TaskOutput> {
            let c = self.n.fetch_add(1, Ordering::SeqCst);
            if c < self.fallo_n {
                anyhow::bail!("fallo transitorio #{}", c + 1);
            }
            Ok(TaskOutput {
                rows: 42,
                message: "ok".into(),
            })
        }
    }

    fn dag_con(fallos: u32) -> (Dag, HashMap<TaskId, Arc<dyn Task>>) {
        let mut dag = Dag::new("retries-test");
        dag.max_retries = 3;
        dag.tasks.push(TaskDef::new("flaky", vec![]));
        let t: Arc<dyn Task> = Arc::new(Flaky {
            n: AtomicU32::new(0),
            fallo_n: fallos,
        });
        (dag, HashMap::from([(id("flaky"), t)]))
    }

    #[test]
    fn reintenta_hasta_que_pasa() {
        let store = Arc::new(Store::open_in_memory().unwrap());
        let engine = Engine::new(Arc::clone(&store)).with_retry_base_ms(1);
        let (dag, tasks) = dag_con(2); // falla 2 veces, pasa en la 3a
        let rep = engine.run(&dag, &tasks, "p1", 1).unwrap();
        assert!(rep.success, "debe pasar tras los reintentos");
        let tr = store.task_runs(rep.run_id).unwrap();
        assert_eq!(tr.len(), 1);
        // el task final esta en Success
        assert!(matches!(tr[0].state, TaskState::Success));
    }

    #[test]
    fn agota_reintentos_y_falla() {
        let store = Arc::new(Store::open_in_memory().unwrap());
        let engine = Engine::new(Arc::clone(&store)).with_retry_base_ms(1);
        let (mut dag, tasks) = dag_con(99); // siempre falla
        dag.max_retries = 2; // 3 intentos en total, todos fallan
        let rep = engine.run(&dag, &tasks, "p2", 1).unwrap();
        assert!(!rep.success);
        let tr = store.task_runs(rep.run_id).unwrap();
        assert!(matches!(tr[0].state, TaskState::Failed { retries_left: 0 }));
    }

    // ---- Cancelación dura (Fase: UI por DAG) ----

    #[test]
    fn cancela_engine_no_lanza_siguiente_task() {
        let store = Arc::new(Store::open_in_memory().unwrap());
        let engine = Engine::new(Arc::clone(&store));

        // "lento" chequea ctx.cancel (que el engine pone desde el store)
        #[derive(Clone)]
        struct Lento;
        impl Task for Lento {
            fn id(&self) -> TaskId {
                id("lento")
            }
            fn run(&self, ctx: &TaskContext) -> anyhow::Result<TaskOutput> {
                for _ in 0..5000 {
                    // hasta 50s si no corta
                    if ctx
                        .cancel
                        .as_ref()
                        .is_some_and(|c| c.load(Ordering::Relaxed))
                    {
                        anyhow::bail!("cancelado");
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Ok(TaskOutput {
                    rows: 1,
                    message: "ok".into(),
                })
            }
        }
        // "corta" cuenta cuando corre
        let corto = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let corto_ref = Arc::clone(&corto);
        #[derive(Clone)]
        struct Corta(Arc<std::sync::atomic::AtomicU32>);
        impl Task for Corta {
            fn id(&self) -> TaskId {
                id("corta")
            }
            fn run(&self, _c: &TaskContext) -> anyhow::Result<TaskOutput> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(TaskOutput {
                    rows: 1,
                    message: "ok".into(),
                })
            }
        }

        let mut dag = Dag::new("cancel-test");
        dag.max_retries = 0; // no reintente si cancela
        dag.tasks.push(TaskDef::new("lento", vec![]));
        dag.tasks.push(TaskDef::new("corta", vec![id("lento")]));
        let tasks: HashMap<TaskId, Arc<dyn Task>> = HashMap::from([
            (id("lento"), Arc::new(Lento) as Arc<dyn Task>),
            (id("corta"), Arc::new(Corta(corto_ref)) as Arc<dyn Task>),
        ]);

        // Lanzo el run en un thread
        let store2 = Arc::clone(&store);
        let engine2 = Arc::new(engine);
        let handle = std::thread::spawn(move || engine2.run(&dag, &tasks, "pc", 2).unwrap());

        // Espera a que el run exista en el store, y cancela
        std::thread::sleep(std::time::Duration::from_millis(200));
        let run_id = store2.latest_running_id().expect("no hay run Running");
        let _ = store2.request_cancel(run_id);

        let rep = handle.join().unwrap();
        assert!(rep.cancelled, "run debe quedar cancelado");
        assert!(!rep.success);
        // "corta" NUNCA debió arrancar
        assert_eq!(
            corto.load(Ordering::SeqCst),
            0,
            "corta no debe ejecutar si se cancela"
        );
        // El run queda terminado (Cancelled) en el store
        assert!(store.run_finished(rep.run_id), "run debe quedar terminado");
    }

    /// Ruta al fixture .wasm (wasm-hola). El fixture se copya al crate en
    /// tests/fixtures/ por el Makefile de release y por el dev.
    #[cfg(feature = "wasm")]
    fn wasm_fixture() -> Option<std::path::PathBuf> {
        let p =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/wasm_hola.wasm");
        p.is_file().then_some(p)
    }

    #[test]
    #[cfg(feature = "wasm")]
    fn wasm_corre_dentro_del_dag() {
        let wasm = wasm_fixture().expect("falta tests/fixtures/wasm_hola.wasm");
        let store = Arc::new(Store::open_in_memory().unwrap());
        let engine = Engine::new(Arc::clone(&store)).with_retry_base_ms(1);

        // DAG: root -> wasm_hola. La tarea wasm se declara en el TaskDef;
        // el hashmap de tareas queda vacio (no hay que registrarla).
        let mut dag = Dag::new("wasm-dag-test");
        dag.max_retries = 0;
        dag.tasks
            .push(TaskDef::wasm("wasm_hola", vec![], WasmSpec::new(wasm)));

        let rep = engine.run(&dag, &HashMap::new(), "hola mundo", 1).unwrap();
        assert!(rep.success, "debe pasar el run con la tarea wasm");
        let tr = store.task_runs(rep.run_id).unwrap();
        assert_eq!(tr.len(), 1);
        assert!(matches!(tr[0].state, TaskState::Success));
        // wasm-hola: rows = param.len() + palabras. "hola mundo" = 10 + 2 = 12
        assert_eq!(rep.total_rows, 12);
        assert_eq!(tr[0].task, "wasm_hola");
    }

    #[test]
    #[cfg(feature = "wasm")]
    fn wasm_en_dag_comparte_orden_topologico_con_rust() {
        let wasm = wasm_fixture().expect("falta tests/fixtures/wasm_hola.wasm");
        let store = Arc::new(Store::open_in_memory().unwrap());
        let engine = Engine::new(Arc::clone(&store)).with_retry_base_ms(1);

        // DAG mixto: rust_task (native) -> wasm_task. El engine debe
        // respetar el orden topologico entre tipos distintos.
        let mut dag = Dag::new("wasm-mixto");
        dag.max_retries = 0;
        dag.tasks.push(TaskDef::new("rust_task", vec![]));
        dag.tasks.push(TaskDef::wasm(
            "wasm_task",
            vec![id("rust_task")],
            WasmSpec::new(wasm),
        ));

        let t: Arc<dyn Task> = Arc::new(Flaky {
            n: AtomicU32::new(0),
            fallo_n: 0,
        });
        let mut tasks = HashMap::new();
        tasks.insert(id("rust_task"), t);

        let rep = engine.run(&dag, &tasks, "x", 1).unwrap();
        assert!(rep.success);
        let tr = store.task_runs(rep.run_id).unwrap();
        assert_eq!(tr.len(), 2);
        // El orden en task_runs sigue el orden topologico: rust_task antes que wasm_task
        let rust_idx = tr.iter().position(|t| t.task == "rust_task").unwrap();
        let wasm_idx = tr.iter().position(|t| t.task == "wasm_task").unwrap();
        assert!(
            rust_idx < wasm_idx,
            "el DAG debe respetar el orden topologico"
        );
    }

    // ---- Tests B.2: SqlTask dentro de un DAG ----

    #[test]
    #[cfg(feature = "sql")]
    fn sql_corre_dentro_del_dag() {
        let tmp = tempfile::tempdir().unwrap();
        // fixture Parquet: 4 filas de ventas
        let ventas = tmp.path().join("ventas.parquet");
        let df = df![
            "fecha" => [1, 2, 3, 4],
            "monto" => [10.0, 20.0, 30.0, 40.0],
            "categoria" => ["a", "b", "a", "b"],
        ]
        .unwrap();
        write_parquet(&df, &ventas);

        let store = Arc::new(Store::open_in_memory().unwrap());
        let engine = Engine::new(Arc::clone(&store)).with_retry_base_ms(1);

        // DAG: una sola tarea SQL, declarada en el TaskDef (hashmap vacio)
        let spec = SqlSpec {
            sql: "SELECT categoria, SUM(monto) AS total FROM ventas WHERE monto > 5 GROUP BY categoria".into(),
            tables: std::collections::HashMap::from([("ventas".to_string(), ventas)]),
            output: tmp.path().join("out.parquet"),
        };
        let mut dag = Dag::new("sql-dag-test");
        dag.max_retries = 0;
        dag.tasks.push(TaskDef::sql("sql_task", vec![], spec));

        let rep = engine.run(&dag, &HashMap::new(), "p", 1).unwrap();
        assert!(rep.success, "debe pasar el run con la tarea sql");
        let tr = store.task_runs(rep.run_id).unwrap();
        assert_eq!(tr.len(), 1);
        assert!(matches!(tr[0].state, TaskState::Success));
        // 2 categorias (a=40, b=60)
        assert_eq!(rep.total_rows, 2);
        assert!(tmp.path().join("out.parquet").is_file());
        let out = read_parquet(&tmp.path().join("out.parquet"));
        assert_eq!(out.height(), 2);
        assert!(out.column("total").is_ok());
    }

    #[test]
    #[cfg(feature = "sql")]
    fn sql_en_dag_despues_de_tarea_native() {
        let tmp = tempfile::tempdir().unwrap();
        let ventas = tmp.path().join("v.parquet");
        let df = df![ "id" => [1, 2, 3] ].unwrap();
        write_parquet(&df, &ventas);

        let store = Arc::new(Store::open_in_memory().unwrap());
        let engine = Engine::new(Arc::clone(&store)).with_retry_base_ms(1);

        // DAG mixto: native_task -> sql_task. La SQL depende de la native.
        let mut dag = Dag::new("sql-mixto");
        dag.max_retries = 0;
        dag.tasks.push(TaskDef::new("native", vec![]));
        dag.tasks.push(TaskDef::sql(
            "sql_task",
            vec![id("native")],
            SqlSpec {
                sql: "SELECT COUNT(*) AS n FROM v".into(),
                tables: std::collections::HashMap::from([("v".to_string(), ventas)]),
                output: tmp.path().join("o.parquet"),
            },
        ));

        let t: Arc<dyn Task> = Arc::new(Flaky {
            n: AtomicU32::new(0),
            fallo_n: 0,
        });
        let mut tasks = HashMap::new();
        tasks.insert(id("native"), t);

        let rep = engine.run(&dag, &tasks, "p", 1).unwrap();
        assert!(rep.success);
        let tr = store.task_runs(rep.run_id).unwrap();
        let native_idx = tr.iter().position(|t| t.task == "native").unwrap();
        let sql_idx = tr.iter().position(|t| t.task == "sql_task").unwrap();
        assert!(native_idx < sql_idx, "la SQL debe correr despues de su dep");
    }

    // ---- Cancelacion entre procesos (N-02) ----

    #[test]
    fn cancela_un_run_pedido_desde_otro_proceso() {
        // El caso real: `zeruel web` cancela un run que corre en `zeruel serve`.
        // Se simula con dos Store sobre el MISMO archivo (dos conexiones), que
        // es exactamente lo que son dos procesos.
        let tmp = tempfile::tempdir().unwrap();
        let store_engine = Arc::new(Store::open(tmp.path()).unwrap());
        let store_ui = Store::open(tmp.path()).unwrap();

        struct Lento;
        impl Task for Lento {
            fn id(&self) -> TaskId {
                id("lento")
            }
            fn run(&self, ctx: &TaskContext) -> anyhow::Result<TaskOutput> {
                for _ in 0..600 {
                    // hasta 60s si no corta
                    if ctx
                        .cancel
                        .as_ref()
                        .is_some_and(|c| c.load(Ordering::Relaxed))
                    {
                        anyhow::bail!("cancelado");
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                Ok(TaskOutput {
                    rows: 1,
                    message: "ok".into(),
                })
            }
        }
        let nunca = Arc::new(AtomicU32::new(0));
        struct Segunda(Arc<AtomicU32>);
        impl Task for Segunda {
            fn id(&self) -> TaskId {
                id("segunda")
            }
            fn run(&self, _c: &TaskContext) -> anyhow::Result<TaskOutput> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(TaskOutput {
                    rows: 1,
                    message: "ok".into(),
                })
            }
        }

        let mut dag = Dag::new("cancel-cross");
        dag.max_retries = 0;
        dag.tasks.push(TaskDef::new("lento", vec![]));
        dag.tasks.push(TaskDef::new("segunda", vec![id("lento")]));
        let tasks: HashMap<TaskId, Arc<dyn Task>> = HashMap::from([
            (id("lento"), Arc::new(Lento) as Arc<dyn Task>),
            (
                id("segunda"),
                Arc::new(Segunda(Arc::clone(&nunca))) as Arc<dyn Task>,
            ),
        ]);

        let engine = Engine::new(Arc::clone(&store_engine));
        let h = std::thread::spawn(move || engine.run(&dag, &tasks, "p", 2).unwrap());

        // El "otro proceso" espera a ver el run y lo cancela.
        let mut run_id = None;
        for _ in 0..50 {
            if let Some(r) = store_ui.latest_running_id() {
                run_id = Some(r);
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        let run_id = run_id.expect("el run tiene que aparecer en la DB");
        assert!(
            store_ui.request_cancel(run_id),
            "la UI marca el run Cancelled"
        );

        let rep = h.join().unwrap();
        assert!(rep.cancelled, "el engine tiene que enterarse por la DB");
        assert!(!rep.success);
        assert_eq!(
            nunca.load(Ordering::SeqCst),
            0,
            "la task siguiente no arranca"
        );
        // Y el estado final NO vuelve a Success/Failed
        assert_eq!(
            store_ui.run_by_id(run_id).unwrap().unwrap().state,
            crate::store::RunState::Cancelled
        );
    }

    // ---- Durabilidad (N-07) ----

    #[test]
    fn retomar_un_param_conserva_las_metricas_originales() {
        let store = Arc::new(Store::open_in_memory().unwrap());
        let engine = Engine::new(Arc::clone(&store)).with_retry_base_ms(1);
        let (dag, tasks) = dag_con(0);

        let r1 = engine.run(&dag, &tasks, "mismo", 1).unwrap();
        assert_eq!(r1.total_rows, 42);
        assert_eq!(r1.skipped, 0);

        // Segundo run del MISMO param: la task se reutiliza.
        let r2 = engine.run(&dag, &tasks, "mismo", 1).unwrap();
        assert_eq!(r2.skipped, 1, "se saltea por durabilidad");
        let tr = store.task_runs(r2.run_id).unwrap();
        assert_eq!(tr[0].rows, 42, "conserva las filas del run donde si corrio");
        assert!(tr[0].message.contains("reutilizado"));

        // Y las stats no se ensucian con la fila reutilizada.
        let stats = store.task_stats().unwrap();
        let f = stats.iter().find(|t| t.task == "flaky").unwrap();
        assert_eq!(f.ejecuciones, 1);
        assert_eq!(f.filas_prom, 42.0);
    }

    #[test]
    fn un_param_distinto_no_retoma_el_run_de_otro() {
        // N-06 visto desde el engine.
        let store = Arc::new(Store::open_in_memory().unwrap());
        let engine = Engine::new(Arc::clone(&store)).with_retry_base_ms(1);
        let (dag, tasks) = dag_con(0);
        // Un run de "A" que quedo Running (simula un kill a mitad)
        let colgado = store.create_run("retries-test", "A").unwrap();

        let rep = engine.run(&dag, &tasks, "B", 1).unwrap();
        assert_ne!(rep.run_id, colgado, "B no puede adoptar el run de A");
        assert_eq!(store.run_by_id(rep.run_id).unwrap().unwrap().param, "B");
    }

    #[test]
    fn dep_inexistente_falla_con_mensaje_claro() {
        // N-14 desde el engine: el error tiene que nombrar la dep.
        let store = Arc::new(Store::open_in_memory().unwrap());
        let engine = Engine::new(Arc::clone(&store));
        let mut dag = Dag::new("roto");
        dag.tasks.push(TaskDef::new("a", vec![]));
        dag.tasks.push(TaskDef::new("b", vec![id("no-existe")]));
        let t: Arc<dyn Task> = Arc::new(Flaky {
            n: AtomicU32::new(0),
            fallo_n: 0,
        });
        let tasks = HashMap::from([(id("a"), Arc::clone(&t)), (id("b"), t)]);
        let err = engine.run(&dag, &tasks, "p", 1).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("no-existe"),
            "el error nombra la dep que falta: {msg}"
        );
    }
}
