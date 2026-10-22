//! Metadata store de zeruel sobre SQLite-WAL.
//! Zero dependencias externas: el db vive en un archivo dentro de `state/`.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Estado de un run en el store.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunState {
    Running,
    Success,
    Failed,
    Cancelled,
}

impl std::fmt::Display for RunState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            RunState::Running => "Running",
            RunState::Success => "Success",
            RunState::Failed => "Failed",
            RunState::Cancelled => "Cancelled",
        })
    }
}

impl RunState {
    /// Parsea el estado como viene de la DB.
    ///
    /// Un solo lugar a proposito (N-01): antes cada lectura hacia su propio
    /// `match` con un `_ => Running` al final, y `Cancelled` caia en ese
    /// comodin. Un run cancelado se mostraba "Running" para siempre en la CLI y
    /// en la UI, y el boton Cancelar seguia apareciendo.
    pub fn parse(s: &str) -> RunState {
        match s {
            "Success" => RunState::Success,
            "Failed" => RunState::Failed,
            "Cancelled" => RunState::Cancelled,
            _ => RunState::Running,
        }
    }

    /// ¿Es un estado terminal? (no va a cambiar mas por si solo)
    pub fn is_final(&self) -> bool {
        !matches!(self, RunState::Running)
    }
}

/// Fila de runs.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Run {
    pub id: Uuid,
    pub dag: String,
    pub param: String,
    pub state: RunState,
    pub started_at: i64, // unix secs
    pub finished_at: Option<i64>,
}

/// Los campos de un checkpoint de tarea.
///
/// Va como struct y no como 8 argumentos sueltos (deuda B-01): con esa firma
/// era facil cruzar `rows` con `ram_peak_mb` sin que el compilador dijera nada.
pub struct TaskStateRecord<'a> {
    pub run_id: Uuid,
    pub task: &'a str,
    pub state: &'a zeruel_sdk::TaskState,
    pub rows: u64,
    pub message: &'a str,
    pub ram_peak_mb: u64,
    pub cpu_secs: f64,
    /// La tarea no se ejecuto en este run: se reutilizo el resultado de uno
    /// anterior (durabilidad). Sus numeros no son mediciones de este run.
    pub reused: bool,
    /// Cuantos intentos consumio (1 = salio a la primera).
    pub intentos: u32,
}

impl<'a> TaskStateRecord<'a> {
    /// Checkpoint de una tarea que SI se ejecuto en este run.
    pub fn medido(
        run_id: Uuid,
        task: &'a str,
        state: &'a zeruel_sdk::TaskState,
        rows: u64,
        message: &'a str,
        ram_peak_mb: u64,
        cpu_secs: f64,
    ) -> Self {
        Self {
            run_id,
            task,
            state,
            rows,
            message,
            ram_peak_mb,
            cpu_secs,
            reused: false,
            intentos: 1,
        }
    }

    /// Como `medido`, pero diciendo en que intento salio.
    #[allow(clippy::too_many_arguments)]
    pub fn con_intentos(
        run_id: Uuid,
        task: &'a str,
        state: &'a zeruel_sdk::TaskState,
        rows: u64,
        message: &'a str,
        ram_peak_mb: u64,
        cpu_secs: f64,
        intentos: u32,
    ) -> Self {
        Self {
            intentos,
            ..Self::medido(run_id, task, state, rows, message, ram_peak_mb, cpu_secs)
        }
    }

    /// Checkpoint de transicion (Running / reintento): sin metricas todavia.
    pub fn transicion(
        run_id: Uuid,
        task: &'a str,
        state: &'a zeruel_sdk::TaskState,
        message: &'a str,
    ) -> Self {
        Self {
            run_id,
            task,
            state,
            rows: 0,
            message,
            ram_peak_mb: 0,
            cpu_secs: 0.0,
            reused: false,
            intentos: 1,
        }
    }
}

/// Fila de task_runs (el checkpoint de durable execution).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskRun {
    pub run_id: Uuid,
    pub task: String,
    pub state: zeruel_sdk::TaskState,
    pub retries_left: u32,
    pub rows: u64,
    pub message: String,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub ram_peak_mb: u64,
    pub cpu_secs: f64,
    /// La tarea no corrio en este run: se reutilizo de uno anterior.
    pub reused: bool,
    /// Cuantos intentos consumio (1 = salio a la primera).
    pub intentos: u32,
}

/// Resultado de una GC (purga de retencion).
#[derive(Clone, Debug, Default, Serialize)]
pub struct GcReport {
    pub runs_borrados: i64,
    pub task_runs_borrados: i64,
    pub meses_agregados: i64,
    pub db_size_bytes: u64,
}

/// Item de la cola global de runs (Fase 4).
#[derive(Clone, Debug, Serialize)]
pub struct QueueItem {
    pub id: i64,
    pub dag: String,
    pub param: String,
    pub priority: i64,
    pub enqueued_at: i64,
    pub state: String,
}

/// Resultado de encolar un run.
#[derive(Clone, Debug, Serialize)]
pub struct EnqueueResult {
    pub ok: bool,
    pub motivo: String,
}

/// Resultado del picking del scheduler: que se lanza y por que se salto al resto.
#[derive(Clone, Debug, Default, Serialize)]
pub struct PickResult {
    pub lanzado: Option<QueueItem>,
    pub bloqueados_conflicto: usize,
    pub no_caben_ram: usize,
    /// El primero que no entro por RAM: (dag, cuanto pide, cuanto habia).
    ///
    /// Contar cuantos no entran no alcanza para hacer nada al respecto. Con el
    /// nombre y los dos numeros, quien lea el log sabe si le falta memoria o si
    /// el historial de ese DAG quedo mal medido.
    pub primero_sin_ram: Option<(String, u64, u64)>,
    /// Se lanzo aunque la estimacion no entrara, porque ya habia esperado
    /// demasiado: (cuanto pedia, cuanto habia, segundos esperando).
    ///
    /// No es un fallo: es la decision deliberada de dejar de postergar. Se
    /// reporta para que quede dicho en el log por que se lanzo algo que "no
    /// entraba".
    pub forzado_por_espera: Option<(u64, u64, i64)>,
}

/// Pulso de higiene del sistema (para `zeruel status`).
#[derive(Clone, Debug, Default, Serialize)]
pub struct Hygiene {
    pub db_size_bytes: u64,
    pub runs: i64,
    pub task_runs: i64,
    pub meses: i64,
    pub ultima_gc: Option<i64>,
    pub db_mb_warning: bool,
}

/// Fila de rollups_monthly (agregado mensual por DAG).
#[derive(Clone, Debug, Serialize)]
pub struct RollupRow {
    pub mes: String,
    pub dag: String,
    pub runs: i64,
    pub ok: i64,
    pub fail: i64,
    /// REAL en SQLite (N-55): leerlo como i64 hacia fallar `rollups()`
    /// con "Invalid column type Real" apenas habia una fila.
    pub wall_secs: f64,
    pub ram_peak_mb: i64,
}

/// Resumen de runs por DAG (lo que muestra la tabla de DAGs de la UI).
#[derive(Clone, Debug, Serialize)]
pub struct DagStat {
    pub dag: String,
    pub runs: i64,
    pub ok: i64,
    pub fail: i64,
    pub cancelled: i64,
    pub running: i64,
    pub ultimo_run: Option<i64>,
}

/// Estadisticas agregadas por task sobre todo el historial de task_runs.
#[derive(Clone, Debug, Serialize)]
pub struct TaskStat {
    pub task: String,
    pub ejecuciones: i64,
    pub filas_prom: f64,
    pub cpu_prom: f64,
    pub ram_peak: i64,
    pub ok: i64,
    pub fail: i64,
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    dag TEXT NOT NULL,
    param TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    finished_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_runs_dag ON runs(dag, started_at);

CREATE TABLE IF NOT EXISTS task_runs (
    run_id TEXT NOT NULL,
    task TEXT NOT NULL,
    state TEXT NOT NULL,
    retries_left INTEGER NOT NULL DEFAULT 0,
    rows INTEGER NOT NULL DEFAULT 0,
    message TEXT NOT NULL DEFAULT '',
    started_at INTEGER,
    finished_at INTEGER,
    ram_peak_mb INTEGER NOT NULL DEFAULT 0,
    cpu_secs REAL NOT NULL DEFAULT 0,
    intentos INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (run_id, task)
);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Agregados mensuales por DAG (sobreviven a la purga de runs viejos)
CREATE TABLE IF NOT EXISTS rollups_monthly (
    mes TEXT NOT NULL,
    dag TEXT NOT NULL,
    runs INTEGER NOT NULL DEFAULT 0,
    ok INTEGER NOT NULL DEFAULT 0,
    fail INTEGER NOT NULL DEFAULT 0,
    wall_secs REAL NOT NULL DEFAULT 0,
    ram_peak_mb INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (mes, dag)
);

-- Cola global de runs (Fase 4): el scheduler (zeruel serve) la consume
CREATE TABLE IF NOT EXISTS queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dag TEXT NOT NULL,
    param TEXT NOT NULL DEFAULT '',
    priority INTEGER NOT NULL DEFAULT 0,
    enqueued_at INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'queued'
);
"#;

pub struct Store {
    pub(crate) conn: Mutex<Connection>,
    pub path: PathBuf,
    /// Senales de cancelacion en memoria: run_id -> flag. La web/scheduler la
    /// setean al cancelar; el engine (entre tasks) y el sandbox wasm (en vivo)
    /// la consultan. No persiste: un run cancelado queda 'Cancelled' en la DB.
    cancels:
        Mutex<std::collections::HashMap<uuid::Uuid, std::sync::Arc<std::sync::atomic::AtomicBool>>>,
}

impl Store {
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("no puedo crear {}", dir.display()))?;
        let path = dir.join("zeruel.db");
        avisar_base_partida(dir);
        let conn = Connection::open(&path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        // Sin esto el default de SQLite es 0: una escritura que encuentra la base
        // tomada por otro proceso devuelve SQLITE_BUSY al instante, en vez de
        // esperar (N-57). Con el scheduler lanzando N pipelines como subprocesos,
        // todos escribiendo checkpoints sobre la misma base, eso se traduciria en
        // runs que fallan con "database is locked" sin ningun motivo real.
        //
        // WAL deja convivir muchos lectores con un escritor, asi que la espera
        // solo aparece entre escrituras, que aca son cortas (un UPSERT por
        // checkpoint). 5 segundos es holgado de sobra.
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        Self::init(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            path,
            cancels: Mutex::new(std::collections::HashMap::new()),
        })
    }

    /// Esquema + migraciones. Un solo camino para el disco y para memoria
    /// (N-38): `open_in_memory` corria solo el SCHEMA, asi que los tests no
    /// pasaban nunca por las migraciones que si corre produccion.
    fn init(conn: &Connection) -> Result<()> {
        conn.execute_batch(SCHEMA)?;
        Self::migrate(conn)
    }

    /// ¿La tabla tiene esta columna? (para migraciones aditivas idempotentes)
    fn tiene_columna(conn: &Connection, tabla: &str, col: &str) -> Result<bool> {
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info(?1) WHERE name = ?2",
            params![tabla, col],
            |r| r.get(0),
        )?;
        Ok(n > 0)
    }

    /// Migraciones aditivas para DBs de versiones anteriores.
    fn migrate(conn: &Connection) -> Result<()> {
        if !Self::tiene_columna(conn, "task_runs", "ram_peak_mb")? {
            conn.execute_batch(
                "ALTER TABLE task_runs ADD COLUMN ram_peak_mb INTEGER NOT NULL DEFAULT 0;
                 ALTER TABLE task_runs ADD COLUMN cpu_secs REAL NOT NULL DEFAULT 0;",
            )?;
        }
        // `reused`: la task se reutilizo de un run anterior (durabilidad) y no
        // se ejecuto en este run. Sus filas/RAM/CPU no son mediciones de este
        // run, asi que quedan fuera de las estadisticas (N-07).
        if !Self::tiene_columna(conn, "task_runs", "reused")? {
            conn.execute_batch(
                "ALTER TABLE task_runs ADD COLUMN reused INTEGER NOT NULL DEFAULT 0;",
            )?;
        }
        // `taken_at`: cuando el scheduler tomo el item de la cola. Permite
        // recuperar items que quedaron 'running' porque el proceso murio (N-41).
        // `intentos`: cuantos intentos consumio la tarea (1 = salio a la
        // primera). `retries_left` no servia: solo se escribe en el registro
        // final y ahi siempre vale 0, asi que una tarea que fallo dos veces y
        // salio a la tercera quedaba registrada igual que una que salio de una.
        // Un DAG que anda a costa de reintentar siempre es un DAG que se esta
        // por romper, y no habia forma de verlo.
        if !Self::tiene_columna(conn, "task_runs", "intentos")? {
            conn.execute_batch(
                "ALTER TABLE task_runs ADD COLUMN intentos INTEGER NOT NULL DEFAULT 1;",
            )?;
        }
        if !Self::tiene_columna(conn, "queue", "taken_at")? {
            conn.execute_batch("ALTER TABLE queue ADD COLUMN taken_at INTEGER;")?;
        }
        Ok(())
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::init(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            path: PathBuf::from(":memory:"),
            cancels: Mutex::new(std::collections::HashMap::new()),
        })
    }

    pub(crate) fn now() -> i64 {
        chrono::Utc::now().timestamp()
    }

    /// Crea un run y devuelve su id.
    pub fn create_run(&self, dag: &str, param: &str) -> Result<Uuid> {
        let id = Uuid::new_v4();
        self.conn.lock().unwrap().execute(
            "INSERT INTO runs (id, dag, param, state, started_at) VALUES (?1, ?2, ?3, 'Running', ?4)",
            params![id.to_string(), dag, param, Self::now()],
        )?;
        Ok(id)
    }

    /// Cambia el estado de un run.
    ///
    /// `Cancelled` gana siempre: un run cancelado no vuelve a Success/Failed
    /// aunque el engine termine su ultima task despues (N-02). Sin esto, el
    /// `set_run_state(Success)` del final del run pisaba la cancelacion que
    /// habia pedido la UI y el run figuraba como exitoso.
    pub fn set_run_state(&self, id: Uuid, state: RunState) -> Result<()> {
        let c = self.conn.lock().unwrap();
        match state {
            RunState::Cancelled => {
                c.execute(
                    "UPDATE runs SET state = 'Cancelled', finished_at = ?2 WHERE id = ?1",
                    params![id.to_string(), Self::now()],
                )?;
            }
            RunState::Success | RunState::Failed => {
                c.execute(
                    "UPDATE runs SET state = ?2, finished_at = ?3
                     WHERE id = ?1 AND state <> 'Cancelled'",
                    params![id.to_string(), state.to_string(), Some(Self::now())],
                )?;
            }
            RunState::Running => {
                c.execute(
                    "UPDATE runs SET state = 'Running', finished_at = NULL
                     WHERE id = ?1 AND state <> 'Cancelled'",
                    params![id.to_string()],
                )?;
            }
        }
        Ok(())
    }

    /// Cancela un run.
    ///
    /// Hace tres cosas, y las tres importan:
    /// 1. Setea la señal en memoria (corta al instante si el run vive en ESTE
    ///    proceso: es lo que usa el sandbox wasm para abortar en vivo).
    /// 2. **Persiste el pedido** en `meta` con la clave `cancel:<run_id>`.
    ///    Sin esto la cancelacion no cruzaba de proceso (N-02): `zeruel web`
    ///    seteaba un flag que solo veia `zeruel web`, mientras el run corria en
    ///    `zeruel serve` y terminaba normal.
    /// 3. Marca el run `Cancelled` en la DB si seguia Running.
    pub fn request_cancel(&self, id: Uuid) -> bool {
        let flag = self.cancel_flag(id);
        flag.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = self.meta_set(&format!("cancel:{id}"), "1");
        self.conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE runs SET state = 'Cancelled', finished_at = ?2
                 WHERE id = ?1 AND state = 'Running'",
                params![id.to_string(), Self::now()],
            )
            .map(|n| n > 0)
            .unwrap_or(false)
    }

    /// ¿Alguien pidio cancelar este run (desde cualquier proceso)?
    ///
    /// El engine la consulta con un watcher periodico y, si da true, prende el
    /// flag en memoria: asi el corte del wasm y el "no arranques la proxima
    /// task" funcionan igual venga el pedido de esta CLI o de otro proceso.
    pub fn cancel_requested(&self, id: Uuid) -> bool {
        matches!(self.meta_get(&format!("cancel:{id}")), Ok(Some(v)) if v == "1")
    }

    /// Limpia el pedido de cancelacion (al terminar el run, para no dejar
    /// claves colgadas en `meta` una por run cancelado).
    pub fn clear_cancel(&self, id: Uuid) {
        let _ = self.meta_del(&format!("cancel:{id}"));
        self.cancels.lock().unwrap().remove(&id);
    }

    /// Señal de cancelacion de un run (para el engine y el sandbox wasm).
    pub fn cancel_flag(&self, id: Uuid) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
        self.cancels
            .lock()
            .unwrap()
            .entry(id)
            .or_insert_with(|| std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)))
            .clone()
    }

    /// Último run que está en Running (para el test de cancelación).
    pub fn latest_running_id(&self) -> Option<Uuid> {
        self.conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT id FROM runs WHERE state = 'Running' ORDER BY started_at DESC LIMIT 1",
                [],
                |r| r.get::<_, String>(0),
            )
            .ok()
            .and_then(|s| Uuid::parse_str(&s).ok())
    }

    /// ¿El run ya termino (Success/Failed/Cancelled)? El engine lo usa para
    /// no re-anclar un run que fue cancelado mientras corria.
    pub fn run_finished(&self, id: Uuid) -> bool {
        self.conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT state FROM runs WHERE id = ?1",
                params![id.to_string()],
                |r| r.get::<_, String>(0),
            )
            .map(|s| s != "Running")
            .unwrap_or(false)
    }

    /// Estado inicial de una tarea en un run: si ya existia un task_run
    /// exitoso en un run anterior del mismo DAG (misma param), se reutiliza:
    /// es el corazon de la durabilidad (no re-ejecutamos lo que termino).
    pub fn task_initial_state(
        &self,
        dag: &str,
        param: &str,
        task: &str,
    ) -> Result<zeruel_sdk::TaskState> {
        let c = self.conn.lock().unwrap();
        let done: Option<String> = c
            .query_row(
                "SELECT tr.state FROM task_runs tr
                 JOIN runs r ON r.id = tr.run_id
                 WHERE r.dag = ?1 AND r.param = ?2 AND tr.task = ?3 AND tr.state = 'Success'
                 ORDER BY r.started_at DESC LIMIT 1",
                params![dag, param, task],
                |row| row.get(0),
            )
            .ok();
        Ok(match done {
            Some(s) if s == "Success" => zeruel_sdk::TaskState::Success,
            _ => zeruel_sdk::TaskState::Pending,
        })
    }

    /// Checkpoint de una tarea. Ver [`TaskStateRecord`] para los campos.
    pub fn set_task_state(&self, rec: TaskStateRecord<'_>) -> Result<()> {
        let TaskStateRecord {
            run_id,
            task,
            state,
            rows,
            message,
            ram_peak_mb,
            cpu_secs,
            reused,
            intentos,
        } = rec;
        let c = self.conn.lock().unwrap();
        let s = match state {
            zeruel_sdk::TaskState::Pending => "Pending",
            zeruel_sdk::TaskState::Running => "Running",
            zeruel_sdk::TaskState::Success => "Success",
            zeruel_sdk::TaskState::Failed { retries_left: _ } => "Failed",
        };
        let started = matches!(state, zeruel_sdk::TaskState::Running);
        let finished = matches!(state, zeruel_sdk::TaskState::Success)
            || matches!(state, zeruel_sdk::TaskState::Failed { .. });
        let retries = match state {
            zeruel_sdk::TaskState::Failed { retries_left } => *retries_left,
            _ => 0,
        };
        c.execute(
            "INSERT INTO task_runs (run_id, task, state, retries_left, rows, message, started_at, finished_at, ram_peak_mb, cpu_secs, reused, intentos)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
             ON CONFLICT(run_id, task) DO UPDATE SET
                state = excluded.state,
                retries_left = excluded.retries_left,
                rows = excluded.rows,
                message = excluded.message,
                started_at = COALESCE(excluded.started_at, task_runs.started_at),
                finished_at = COALESCE(excluded.finished_at, task_runs.finished_at),
                ram_peak_mb = MAX(excluded.ram_peak_mb, task_runs.ram_peak_mb),
                cpu_secs = MAX(excluded.cpu_secs, task_runs.cpu_secs),
                reused = excluded.reused,
                -- El intento mas alto gana: la fila se pisa en cada reintento y
                -- lo que interesa es cuantos hicieron falta en total.
                intentos = MAX(excluded.intentos, task_runs.intentos)",
            params![
                run_id.to_string(),
                task,
                s,
                retries,
                rows,
                message,
                started.then(Self::now),
                finished.then(Self::now),
                ram_peak_mb,
                cpu_secs,
                reused as i64,
                intentos
            ],
        )?;
        Ok(())
    }

    /// Ultimo resultado exitoso de una tarea (para reutilizarlo al retomar un
    /// run): filas, RAM y CPU medidos cuando SI se ejecuto.
    pub fn last_success_metrics(
        &self,
        dag: &str,
        param: &str,
        task: &str,
    ) -> Option<(u64, u64, f64)> {
        let c = self.conn.lock().unwrap();
        c.query_row(
            "SELECT tr.rows, tr.ram_peak_mb, tr.cpu_secs FROM task_runs tr
             JOIN runs r ON r.id = tr.run_id
             WHERE r.dag = ?1 AND r.param = ?2 AND tr.task = ?3
               AND tr.state = 'Success' AND tr.reused = 0
             ORDER BY r.started_at DESC LIMIT 1",
            params![dag, param, task],
            |r| {
                Ok((
                    r.get::<_, i64>(0)? as u64,
                    r.get::<_, i64>(1)? as u64,
                    r.get::<_, f64>(2)?,
                ))
            },
        )
        .ok()
    }

    /// Re-levar tras un kill: devuelve el run Running de ESTE dag+param (si hay
    /// uno) para retomarlo desde donde estaba.
    ///
    /// El `param` no es opcional (N-06): antes se buscaba solo por dag, asi que
    /// un run colgado de `--param A` se "retomaba" al correr `--param B` y los
    /// checkpoints de B terminaban dentro del run de A.
    pub fn recover_running(&self, dag: &str, param: &str) -> Result<Option<Uuid>> {
        let c = self.conn.lock().unwrap();
        Ok(c.query_row(
            "SELECT id FROM runs
                 WHERE dag = ?1 AND param = ?2 AND state = 'Running'
                 ORDER BY started_at DESC LIMIT 1",
            params![dag, param],
            |row| row.get::<_, String>(0),
        )
        .ok()
        .map(|s| Uuid::parse_str(&s))
        .transpose()?)
    }

    /// Task runs de un run (para status y UI).
    pub fn task_runs(&self, run_id: Uuid) -> Result<Vec<TaskRun>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare(
            "SELECT task, state, retries_left, rows, message, started_at, finished_at, ram_peak_mb, cpu_secs, reused, intentos
             FROM task_runs WHERE run_id = ?1 ORDER BY rowid",
        )?;
        let rows = stmt.query_map(params![run_id.to_string()], |r| {
            let st: String = r.get(1)?;
            Ok(TaskRun {
                run_id,
                task: r.get(0)?,
                state: match st.as_str() {
                    "Success" => zeruel_sdk::TaskState::Success,
                    "Running" => zeruel_sdk::TaskState::Running,
                    "Failed" => zeruel_sdk::TaskState::Failed {
                        retries_left: r.get(2)?,
                    },
                    _ => zeruel_sdk::TaskState::Pending,
                },
                retries_left: r.get(2)?,
                rows: r.get(3)?,
                message: r.get(4)?,
                started_at: r.get(5)?,
                finished_at: r.get(6)?,
                ram_peak_mb: r.get(7)?,
                cpu_secs: r.get(8)?,
                intentos: r.get(10)?,
                reused: r.get::<_, i64>(9)? != 0,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }
    pub fn recent_runs(&self, limit: usize, offset: usize) -> Result<Vec<Run>> {
        self.recent_runs_de(None, limit, offset)
    }

    /// Las corridas mas recientes, opcionalmente de un solo DAG.
    ///
    /// El filtro va en el SQL y NO despues, que es como estaba antes y por eso
    /// un DAG con corridas se veia como si nunca hubiera corrido: se traian las
    /// ultimas N de *todos* los DAGs y se filtraba sobre eso. Con 56 DAGs, las
    /// 100 mas recientes las copan los que corren cada diez minutos, y
    /// cualquier otro devolvia una lista vacia.
    ///
    /// El sintoma era peor que una tabla vacia: la pantalla decia "este DAG
    /// nunca corrio" cuando en realidad habia corrido y fallado, que es
    /// justamente el momento en que alguien la esta mirando.
    pub fn recent_runs_de(
        &self,
        dag: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Run>> {
        let c = self.conn.lock().unwrap();
        let (sql, args): (&str, Vec<Box<dyn rusqlite::ToSql>>) = match dag {
            Some(d) => (
                "SELECT id, dag, param, state, started_at, finished_at FROM runs                  WHERE dag = ?1 ORDER BY started_at DESC LIMIT ?2 OFFSET ?3",
                vec![Box::new(d.to_string()), Box::new(limit as i64), Box::new(offset as i64)],
            ),
            None => (
                "SELECT id, dag, param, state, started_at, finished_at FROM runs                  ORDER BY started_at DESC LIMIT ?1 OFFSET ?2",
                vec![Box::new(limit as i64), Box::new(offset as i64)],
            ),
        };
        let mut stmt = c.prepare(sql)?;
        let refs: Vec<&dyn rusqlite::ToSql> = args.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(refs.as_slice(), |r| {
            let st: String = r.get(3)?;
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                st,
                r.get::<_, i64>(4)?,
                r.get::<_, Option<i64>>(5)?,
            ))
        })?;
        let mut out = Vec::new();
        for r in rows {
            let (id_s, dag, param, st, started_at, finished_at) = r?;
            out.push(Run {
                id: Uuid::parse_str(&id_s).context("id de run invalido")?,
                dag,
                param,
                state: RunState::parse(&st),
                started_at,
                finished_at,
            });
        }
        Ok(out)
    }

    /// Un run puntual por id. Es lo que hay que usar para "mostrame este run":
    /// antes la CLI, la web y el MCP se traian los N runs mas recientes y
    /// buscaban con un `find` lineal, asi que un run viejo "no existia"
    /// (N-13) y `zeruel status --run X` devolvia el header de otro run (N-03).
    pub fn run_by_id(&self, id: Uuid) -> Result<Option<Run>> {
        let c = self.conn.lock().unwrap();
        let r = c
            .query_row(
                "SELECT id, dag, param, state, started_at, finished_at FROM runs WHERE id = ?1",
                params![id.to_string()],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, i64>(4)?,
                        r.get::<_, Option<i64>>(5)?,
                    ))
                },
            )
            .ok();
        Ok(match r {
            Some((id_s, dag, param, st, started_at, finished_at)) => Some(Run {
                id: Uuid::parse_str(&id_s).context("id de run invalido")?,
                dag,
                param,
                state: RunState::parse(&st),
                started_at,
                finished_at,
            }),
            None => None,
        })
    }

    /// Tamaño real de la base: el `.db` mas el `-wal` y el `-shm`.
    ///
    /// En modo WAL buena parte de lo recien escrito vive en el `-wal` hasta el
    /// checkpoint, asi que mirar solo el `.db` subestima (N-39) — y el warning
    /// de "DB > 512MB" y el KPI de higiene salen de aca.
    fn db_size(&self) -> u64 {
        let main = self.path.metadata().map(|m| m.len()).unwrap_or(0);
        let extra: u64 = ["-wal", "-shm"]
            .iter()
            .map(|suf| {
                let mut p = self.path.clone().into_os_string();
                p.push(suf);
                std::path::PathBuf::from(p)
                    .metadata()
                    .map(|m| m.len())
                    .unwrap_or(0)
            })
            .sum();
        main + extra
    }

    /// Valor de una clave del `meta` (KV generico; p. ej. ultimo disparo cron).
    /// ¿Este DAG esta pausado?
    ///
    /// Pausar significa **dejar de agendarlo**: el cron no dispara. NO frena lo
    /// que ya esta en cola ni mata lo que esta corriendo — para eso esta
    /// cancelar, que es otra accion. Es lo mismo que hace Airflow, y es lo que
    /// se espera: nadie pausa un DAG queriendo matar el ETL de dos horas que
    /// esta a la mitad.
    ///
    /// El estado vive en `meta`, igual que los pedidos de cancelacion, asi que
    /// cruza procesos: se puede pausar desde la web algo que agenda el
    /// scheduler.
    ///
    /// Importante: pausar **no borra el horario**. El cron vive en el
    /// `--describe` del binario; esto solo lo suspende. Al activarlo de nuevo
    /// vuelve el horario que tenia, sin que nadie lo reescriba.
    pub fn dag_pausado(&self, dag: &str) -> bool {
        matches!(self.meta_get(&clave_pausa(dag)), Ok(Some(v)) if v == "1")
    }

    pub fn pausar_dag(&self, dag: &str) -> Result<()> {
        self.meta_set(&clave_pausa(dag), "1")
    }

    /// Se borra la clave en vez de guardar "0": asi `meta` no acumula una fila
    /// por cada DAG que alguna vez se pauso.
    pub fn activar_dag(&self, dag: &str) -> Result<()> {
        self.meta_del(&clave_pausa(dag))?;
        Ok(())
    }

    /// Los DAGs pausados, para no preguntar uno por uno en cada tick.
    pub fn dags_pausados(&self) -> Result<Vec<String>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare("SELECT key FROM meta WHERE key LIKE 'pausa:%'")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        Ok(rows
            .flatten()
            .filter_map(|k| k.strip_prefix("pausa:").map(|s| s.to_string()))
            .collect())
    }

    /// ¿Esta alerta ya se cerro? Devuelve cuando.
    ///
    /// Las alertas no se guardan: se **derivan** del estado en cada consulta
    /// (corridas fallidas de las ultimas 24 h, items pasados de SLA, tamaño de
    /// la base). No hay una fila que borrar — esconder una sin mas la haria
    /// volver en el refresco siguiente, porque la corrida sigue fallida.
    ///
    /// Por eso cerrarla es marcarla como **vista**, y la clave importa:
    ///
    /// - Para una corrida fallida, la clave es su UUID. Una corrida es
    ///   inmutable, asi que cerrarla vale para siempre — y si el DAG vuelve a
    ///   fallar, esa es OTRA corrida, con otro UUID, y aparece una alerta
    ///   nueva. Que es exactamente lo que uno quiere.
    /// - Para la base grande no hay id, y la condicion sigue ahi hasta que
    ///   corra la limpieza: se silencia por un rato, no para siempre.
    pub fn alerta_vista(&self, clave: &str) -> Option<i64> {
        self.meta_get(&format!("alerta_vista:{clave}"))
            .ok()
            .flatten()
            .and_then(|v| v.parse().ok())
    }

    pub fn cerrar_alerta(&self, clave: &str, cuando: i64) -> Result<()> {
        self.meta_set(&format!("alerta_vista:{clave}"), &cuando.to_string())
    }

    /// Borra las marcas viejas.
    ///
    /// Se acumula una por alerta cerrada. Como las alertas de corrida solo
    /// miran las ultimas 24 h, una marca de hace una semana ya no filtra nada:
    /// es una fila que no sirve para nada.
    pub fn purgar_alertas_vistas(&self, antes_de: i64) -> Result<usize> {
        let c = self.conn.lock().unwrap();
        Ok(c.execute(
            "DELETE FROM meta WHERE key LIKE 'alerta_vista:%' AND CAST(value AS INTEGER) < ?1",
            params![antes_de],
        )?)
    }

    pub fn meta_get(&self, key: &str) -> Result<Option<String>> {
        let c = self.conn.lock().unwrap();
        Ok(
            c.query_row("SELECT value FROM meta WHERE key = ?1", params![key], |r| {
                r.get::<_, String>(0)
            })
            .ok(),
        )
    }

    /// Guarda (upsert) el valor de una clave del `meta`.
    pub fn meta_set(&self, key: &str, value: &str) -> Result<()> {
        let c = self.conn.lock().unwrap();
        c.execute(
            "INSERT INTO meta (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    /// Borra una clave del `meta`. Devuelve si existia.
    ///
    /// Hace falta para que "sin tope" sea de verdad la ausencia de la clave y
    /// no un `""` guardado (N-12): el SLA en 0 significaba "tope de 0 segundos"
    /// por CLI y "borrar el tope" por MCP.
    pub fn meta_del(&self, key: &str) -> Result<bool> {
        let c = self.conn.lock().unwrap();
        Ok(c.execute("DELETE FROM meta WHERE key = ?1", params![key])? > 0)
    }

    /// Purga de retencion (Fase 3.8):
    ///
    /// - detalle (task_runs) de runs > 30d -> borrar
    /// - runs > 1a -> borrar, despues de agregar sus stats al rollup mensual
    /// - rollups_monthly -> nunca se borra (historico para siempre)
    ///
    /// Los agregados mensuales se recalculan para **todos los meses ya
    /// cerrados**, no solo para los runs que estan por borrarse (N-40): antes
    /// `rollups_monthly` recien tenia datos cuando habia runs de mas de un año,
    /// asi que la tab Metricas de la UI estaba vacia por diseño durante el
    /// primer año de uso.
    ///
    /// Con `dry_run` solo cuenta lo que borraria.
    pub fn gc(&self, dry_run: bool) -> Result<GcReport> {
        self.gc_con_retencion(dry_run, RETENCION_DETALLE_DIAS, RETENCION_RUNS_DIAS)
    }

    /// Igual, pero eligiendo cuanto se guarda.
    ///
    /// Se separo de `gc` porque la retencion es una decision de despliegue, no
    /// del motor: una maquina chica querra quedarse con una semana de detalle y
    /// un servidor con disco de sobra con varios meses. Estaba fija en el
    /// codigo, asi que cambiarla obligaba a recompilar.
    pub fn gc_con_retencion(
        &self,
        dry_run: bool,
        detalle_dias: i64,
        runs_dias: i64,
    ) -> Result<GcReport> {
        let c = self.conn.lock().unwrap();
        let now = Self::now();
        // El detalle no puede sobrevivir a su run: seria borrar el encabezado y
        // dejar las tareas huerfanas, que no se pueden mostrar en ningun lado.
        let detalle_dias = detalle_dias.clamp(1, runs_dias.max(1));
        let cutoff_detail = now - (detalle_dias * 86_400);
        let cutoff_runs = now - (runs_dias.max(1) * 86_400);
        let mut rep = GcReport::default();

        if dry_run {
            rep.runs_borrados = c.query_row(
                "SELECT COUNT(*) FROM runs WHERE started_at < ?1",
                params![cutoff_runs],
                |r| r.get(0),
            )?;
            rep.task_runs_borrados = c.query_row(
                "SELECT COUNT(*) FROM task_runs tr
                     WHERE tr.run_id IN (SELECT id FROM runs WHERE started_at < ?1)",
                params![cutoff_detail],
                |r| r.get(0),
            )?;
        } else {
            // 1. Consolidar los meses YA CERRADOS (todos los que no son el mes
            //    en curso). Es un REPLACE por (mes, dag) recalculado desde
            //    `runs`, no un acumulado: correr la GC dos veces seguidas da el
            //    mismo resultado, y el mes se puede recalcular mientras sus
            //    runs sigan existiendo.
            let mes_actual = "strftime('%Y-%m', 'now')";
            c.execute(
                &format!(
                    "INSERT OR REPLACE INTO rollups_monthly (mes, dag, runs, ok, fail, wall_secs, ram_peak_mb)
                     SELECT strftime('%Y-%m', r.started_at, 'unixepoch') AS mes,
                            r.dag,
                            COUNT(*),
                            SUM(CASE WHEN r.state='Success' THEN 1 ELSE 0 END),
                            SUM(CASE WHEN r.state='Failed' THEN 1 ELSE 0 END),
                            COALESCE(SUM(COALESCE(r.finished_at, r.started_at) - r.started_at), 0),
                            COALESCE((SELECT MAX(tr.ram_peak_mb) FROM task_runs tr WHERE tr.run_id = r.id), 0)
                     FROM runs r
                     WHERE strftime('%Y-%m', r.started_at, 'unixepoch') < {mes_actual}
                     GROUP BY mes, r.dag"
                ),
                [],
            )?;
            rep.meses_agregados = c.changes() as i64;

            // 2. Borrar detalle (task_runs) de runs > 30d
            rep.task_runs_borrados = c.execute(
                "DELETE FROM task_runs WHERE run_id IN (SELECT id FROM runs WHERE started_at < ?1)",
                params![cutoff_detail],
            )? as i64;

            // 3. Borrar la fila de los runs > 1a (su detalle ya no existe)
            rep.runs_borrados = c.execute(
                "DELETE FROM runs WHERE started_at < ?1",
                params![cutoff_runs],
            )? as i64;

            // 4. Registrar el momento de esta GC
            c.execute(
                "INSERT INTO meta (key, value) VALUES ('last_gc', ?1)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                params![now.to_string()],
            )?;
        }
        rep.db_size_bytes = self.db_size();
        // Las marcas de alerta cerrada se acumulan de a una por alerta. Como
        // las alertas de corrida solo miran las ultimas 24 h, una marca de hace
        // una semana ya no filtra nada: es una fila que no sirve.
        //
        // Va fuera del lock: `purgar_alertas_vistas` lo vuelve a tomar.
        drop(c);
        if !dry_run {
            let _ = self.purgar_alertas_vistas(now - (7 * 86_400));
        }

        Ok(rep)
    }

    /// Agregados mensuales por DAG (sobreviven a la purga).
    /// Devuelve filas ordenadas por mes desc, dag asc.
    pub fn rollups(&self) -> Result<Vec<RollupRow>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare(
            "SELECT mes, dag, runs, ok, fail, wall_secs, ram_peak_mb
             FROM rollups_monthly ORDER BY mes DESC, dag ASC",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(RollupRow {
                mes: r.get(0)?,
                dag: r.get(1)?,
                runs: r.get(2)?,
                ok: r.get(3)?,
                fail: r.get(4)?,
                wall_secs: r.get(5)?,
                ram_peak_mb: r.get(6)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    /// Stats por task sobre el historial de task_runs: ejecuciones, filas
    /// promedio, cpu promedio, picos de RAM.
    ///
    /// Excluye las filas `reused = 1` (N-07): son tareas que no corrieron en
    /// ese run (se reutilizo el resultado por durabilidad). Contarlas hundia
    /// los promedios de la tab Metricas cada vez que se re-corria un param.
    pub fn task_stats(&self) -> Result<Vec<TaskStat>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare(
            "SELECT task, COUNT(*), COALESCE(AVG(rows),0),
                    COALESCE(AVG(cpu_secs),0), COALESCE(MAX(ram_peak_mb),0),
                    SUM(CASE WHEN state='Success' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN state='Failed' THEN 1 ELSE 0 END)
             FROM task_runs WHERE reused = 0 GROUP BY task ORDER BY task ASC",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(TaskStat {
                task: r.get(0)?,
                ejecuciones: r.get(1)?,
                filas_prom: r.get(2)?,
                cpu_prom: r.get(3)?,
                ram_peak: r.get(4)?,
                ok: r.get(5)?,
                fail: r.get(6)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    /// Todos los SLA configurados (clave sla:<dag>).
    pub fn sla_config(&self) -> Result<std::collections::BTreeMap<String, i64>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare("SELECT key, value FROM meta WHERE key LIKE 'sla:%'")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        let mut out = std::collections::BTreeMap::new();
        for (k, v) in rows.flatten() {
            if let Some(dag) = k.strip_prefix("sla:") {
                if let Ok(n) = v.parse::<i64>() {
                    out.insert(dag.to_string(), n);
                }
            }
        }
        Ok(out)
    }

    /// Resumen por DAG: cuantos runs, cuantos OK / fail / cancelados / en curso.
    ///
    /// Lo resuelve SQLite con un GROUP BY. La web lo hacia trayendo los 10.000
    /// runs mas recientes a memoria y contando en Rust, cada 2 segundos (N-36).
    pub fn dag_stats(&self) -> Result<Vec<DagStat>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare(
            "SELECT dag,
                    COUNT(*),
                    SUM(CASE WHEN state='Success'   THEN 1 ELSE 0 END),
                    SUM(CASE WHEN state='Failed'    THEN 1 ELSE 0 END),
                    SUM(CASE WHEN state='Cancelled' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN state='Running'   THEN 1 ELSE 0 END),
                    MAX(started_at)
             FROM runs GROUP BY dag ORDER BY dag ASC",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(DagStat {
                dag: r.get(0)?,
                runs: r.get(1)?,
                ok: r.get(2)?,
                fail: r.get(3)?,
                cancelled: r.get(4)?,
                running: r.get(5)?,
                ultimo_run: r.get(6)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    /// Runs fallidos desde un timestamp (para las alertas de las ultimas 24h).
    /// Filtra en SQL: la web se traia 2.000 runs para quedarse con los de un dia.
    pub fn failed_since(&self, desde: i64) -> Result<Vec<Run>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare(
            "SELECT id, dag, param, state, started_at, finished_at FROM runs
             WHERE state = 'Failed' AND started_at >= ?1
             ORDER BY started_at DESC",
        )?;
        let rows = stmt.query_map(params![desde], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, Option<i64>>(5)?,
            ))
        })?;
        let mut out = Vec::new();
        for r in rows {
            let (id_s, dag, param, st, started_at, finished_at) = r?;
            out.push(Run {
                id: Uuid::parse_str(&id_s).context("id de run invalido")?,
                dag,
                param,
                state: RunState::parse(&st),
                started_at,
                finished_at,
            });
        }
        Ok(out)
    }

    /// Pulso de higiene (para `zeruel status`): tamano DB, contadores, ultima GC,
    /// y un warning si el DB pasa el umbral.
    pub fn hygiene(&self) -> Result<Hygiene> {
        let c = self.conn.lock().unwrap();
        let runs = c.query_row("SELECT COUNT(*) FROM runs", [], |r| r.get::<_, i64>(0))?;
        let task_runs =
            c.query_row("SELECT COUNT(*) FROM task_runs", [], |r| r.get::<_, i64>(0))?;
        let meses = c.query_row("SELECT COUNT(*) FROM rollups_monthly", [], |r| {
            r.get::<_, i64>(0)
        })?;
        let ultima: Option<String> = c
            .query_row("SELECT value FROM meta WHERE key = 'last_gc'", [], |r| {
                r.get::<_, String>(0)
            })
            .ok();
        let ultima_gc = ultima.and_then(|s| s.parse::<i64>().ok());
        let size = self.db_size();
        Ok(Hygiene {
            db_size_bytes: size,
            runs,
            task_runs,
            meses,
            ultima_gc,
            db_mb_warning: size > 512 * 1024 * 1024,
        })
    }

    /// Encola un run para el scheduler (Fase 4). Run conflict policy por defecto:
    /// si ya hay un run activo o otro run en cola del mismo DAG+param, se rechaza.
    pub fn enqueue(&self, dag: &str, param: &str, priority: i64) -> Result<EnqueueResult> {
        let c = self.conn.lock().unwrap();
        let en_cola: i64 = c.query_row(
            "SELECT COUNT(*) FROM queue WHERE dag=?1 AND param=?2 AND state='queued'",
            params![dag, param],
            |r| r.get(0),
        )?;
        if en_cola > 0 {
            return Ok(EnqueueResult {
                ok: false,
                motivo: "ya hay un run de este DAG+param en cola".into(),
            });
        }
        let activo: i64 = c.query_row(
            "SELECT COUNT(*) FROM runs WHERE dag=?1 AND param=?2 AND state='Running'",
            params![dag, param],
            |r| r.get(0),
        )?;
        if activo > 0 {
            return Ok(EnqueueResult {
                ok: false,
                motivo: "ya hay un run activo de este DAG+param (conflict: bloquear)".into(),
            });
        }
        // Cola acotada: tope global de items en cola (meta queue.max_size; 0 = sin tope).
        let max: i64 = c
            .query_row(
                "SELECT value FROM meta WHERE key='queue.max_size'",
                [],
                |r| r.get::<_, String>(0),
            )
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);
        if max > 0 {
            let total: i64 =
                c.query_row("SELECT COUNT(*) FROM queue WHERE state='queued'", [], |r| {
                    r.get(0)
                })?;
            if total >= max {
                return Ok(EnqueueResult {
                    ok: false,
                    motivo: format!("cola llena ({total}/{max} items en cola); sube el tope con `zeruel cola --max N` o espera a que libre"),
                });
            }
        }
        c.execute(
            "INSERT INTO queue (dag, param, priority, enqueued_at, state) VALUES (?1, ?2, ?3, ?4, 'queued')",
            params![dag, param, priority, Self::now()],
        )?;
        Ok(EnqueueResult {
            ok: true,
            motivo: format!("encolado (id={})", c.last_insert_rowid()),
        })
    }

    /// Items de la cola (para `zeruel status`, el semaforo).
    pub fn queue_items(&self) -> Result<Vec<QueueItem>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare(
            "SELECT id, dag, param, priority, enqueued_at, state FROM queue ORDER BY id",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(QueueItem {
                id: r.get(0)?,
                dag: r.get(1)?,
                param: r.get(2)?,
                priority: r.get(3)?,
                enqueued_at: r.get(4)?,
                state: r.get(5)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    /// Cuanta RAM hay que reservarle a un DAG, estimada de su historia.
    ///
    /// # Por que NO es el maximo
    ///
    /// Era `MAX(ram_peak_mb)` sobre toda la historia, y eso hace que **una sola
    /// medicion mala bloquee al DAG para siempre**. No es teorico: hubo un bug
    /// que le atribuia a la tarea el pico de memoria del sistema operativo
    /// entero, y dejo tres corridas del 31/08/2026 grabadas con ~16,9 GB.
    ///
    /// Con el maximo, el control de admision le pedia a `consolidado` 16866 MB
    /// mas 20% = 20.239 MB. La maquina tenia 17.592 MB libres, asi que no
    /// entraba nunca. Y como no corria nunca, **jamas volvia a registrar un
    /// valor bueno**: el bloqueo se sostenia solo. Ocho items estuvieron 1,5
    /// horas en cola sin que nada los tocara.
    ///
    /// El percentil 95 aguanta unos pocos valores absurdos y sigue siendo
    /// conservador: descarta el 5% mas alto, no la mitad. Con pocas corridas
    /// termina siendo el maximo igual, que es lo prudente cuando no se sabe.
    fn dag_ram_peak(c: &Connection, dag: &str) -> u64 {
        let n: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM task_runs tr
                 JOIN runs r ON r.id = tr.run_id
                 WHERE r.dag = ?1 AND tr.ram_peak_mb > 0",
                params![dag],
                |r| r.get(0),
            )
            .unwrap_or(0);

        if n > 0 {
            // El valor que esta en la posicion `salto` contando desde el mas
            // alto. 0 = el maximo, n/20 = el percentil 95, n/2 = la mediana.
            let en = |salto: i64| -> i64 {
                c.query_row(
                    "SELECT tr.ram_peak_mb FROM task_runs tr
                     JOIN runs r ON r.id = tr.run_id
                     WHERE r.dag = ?1 AND tr.ram_peak_mb > 0
                     ORDER BY tr.ram_peak_mb DESC LIMIT 1 OFFSET ?2",
                    params![dag, salto],
                    |r| r.get(0),
                )
                .unwrap_or(0)
            };

            // El p95 descarta el 5% mas alto. Con menos de 20 mediciones eso es
            // cero filas, asi que solo no alcanza: `reporte_mensual` tenia 13
            // corridas y una sola disparatada, y seguia bloqueado.
            let p95 = en(n / 20);

            // Por eso ademas se acota contra la MEDIANA, que no se mueve por
            // unos pocos valores absurdos. Un pico 8 veces mayor que el tipico
            // de ese mismo DAG no es una medicion, es un error de medicion.
            //
            // Ser optimista aca es barato y ser pesimista es carisimo: esto
            // decide a QUIEN se lanza, no cuanta memoria se le permite. De que
            // un run desbocado no se lleve la maquina se encarga `ram_max`, que
            // es un techo de verdad y actua mientras corre. Equivocarse para
            // arriba frena un DAG PARA SIEMPRE -- y ademas se sostiene solo,
            // porque sin correr nunca vuelve a medir bien.
            let mediana = en(n / 2);
            let estimado = if mediana > 0 {
                p95.min(mediana * 8)
            } else {
                p95
            };
            if estimado > 0 {
                return estimado as u64;
            }
        }

        c.query_row(
            "SELECT COALESCE(MAX(ram_peak_mb), 0) FROM rollups_monthly WHERE dag = ?1",
            params![dag],
            |r| r.get(0),
        )
        .unwrap_or(0)
        .max(0) as u64
    }

    /// Pide el proximo run que se lanza (Fase 4). Orden: prioridad efectiva
    /// DESC (base + aging), luego FIFO. Run conflict: bloquea si el mismo
    /// DAG+param ya corre. Admission control: salta si el RAM peak historico
    /// del DAG + margen 20% > RAM disponible.
    ///
    /// `espera_max_ram_segs` es el plazo despues del cual un item deja de
    /// postergarse por memoria y **se lanza igual**. 0 lo desactiva.
    ///
    /// # Por que el control de admision no puede postergar para siempre
    ///
    /// Que un item quede encolado indefinidamente no protege nada: no corre, no
    /// falla, no avisa, y no libera su lugar en la cola. Es el peor de los
    /// finales posibles, porque desde afuera se ve igual que un sistema sano
    /// con trabajo pendiente.
    ///
    /// La estimacion de memoria es **una prediccion**, y las predicciones se
    /// equivocan: basta una medicion mala en el historial para que un DAG no
    /// entre nunca — y como no corre, tampoco vuelve a medir bien.
    ///
    /// Quien de verdad protege a la maquina no es esta cuenta: es `ram_max`,
    /// que actua MIENTRAS el run corre y lo corta apenas se pasa. Asi que
    /// pasado el plazo se lanza, y si efectivamente se excede, se corta con un
    /// diagnostico que dice cuanto uso y cual era el limite — que es
    /// informacion util, al reves que esperar en silencio.
    pub fn pick_next(
        &self,
        running_keys: &[String],
        ram_disponible_mb: u64,
        aging_segs: i64,
        margin_pct: u32,
        espera_max_ram_segs: i64,
    ) -> Result<PickResult> {
        let c = self.conn.lock().unwrap();
        let now = Self::now();
        let mut out = PickResult::default();
        let mut stmt = c.prepare(
            "SELECT id, dag, param, priority, enqueued_at FROM queue WHERE state = 'queued' ORDER BY id",
        )?;
        let items: Vec<(i64, String, String, i64, i64)> = stmt
            .query_map([], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut cands: Vec<(i64, QueueItem)> = items
            .iter()
            .map(|(id, dag, param, pri, enq)| {
                let age = (now - enq) / aging_segs.max(1);
                let peff = pri + age;
                (
                    peff,
                    QueueItem {
                        id: *id,
                        dag: dag.clone(),
                        param: param.clone(),
                        priority: *pri,
                        enqueued_at: *enq,
                        state: "queued".into(),
                    },
                )
            })
            .collect();
        cands.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.enqueued_at.cmp(&b.1.enqueued_at)));
        for (_peff, q) in cands {
            let clave = format!("{}|{}", q.dag, q.param);
            if running_keys.iter().any(|k| k == &clave) {
                out.bloqueados_conflicto += 1;
                continue;
            }
            // Admission control: el pico historico del DAG mas el margen tiene
            // que entrar en la RAM disponible. La cuenta es la que documenta el
            // proyecto — `peak * (1 + margen)` — y no la que estaba antes
            // (`peak > disponible * (1 - margen)`), que no es lo mismo (N-23).
            let peak = Self::dag_ram_peak(&c, &q.dag);
            let necesita = peak.saturating_add(peak * margin_pct as u64 / 100);
            if necesita > ram_disponible_mb {
                let esperando = now - q.enqueued_at;
                if espera_max_ram_segs <= 0 || esperando < espera_max_ram_segs {
                    out.no_caben_ram += 1;
                    if out.primero_sin_ram.is_none() {
                        out.primero_sin_ram = Some((q.dag.clone(), necesita, ram_disponible_mb));
                    }
                    continue;
                }
                // Espero demasiado: se deja de postergar. Lo que sigue es el
                // camino normal de lanzado; `ram_max` se encarga si se pasa.
                out.forzado_por_espera = Some((necesita, ram_disponible_mb, esperando));
            }
            c.execute(
                "UPDATE queue SET state = 'running', taken_at = ?2 WHERE id = ?1",
                params![q.id, now],
            )?;
            out.lanzado = Some(q);
            break;
        }
        Ok(out)
    }

    /// Claves "dag|param" de los runs Running (para run conflict).
    pub fn running_keys(&self) -> Result<Vec<String>> {
        let c = self.conn.lock().unwrap();
        let mut stmt = c.prepare("SELECT dag || '|' || param FROM runs WHERE state = 'Running'")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    /// Al terminar (o fallar) el run, se retira el item de la cola (el run queda
    /// registrado en `runs` con su estado).
    pub fn complete_queue(&self, id: i64) -> Result<()> {
        self.conn
            .lock()
            .unwrap()
            .execute("DELETE FROM queue WHERE id = ?1", params![id])?;
        Ok(())
    }

    /// Devuelve a la cola un item que quedo tomado (`state='running'`).
    /// Se usa cuando el run que lo tomo termino mal y hay que reintentarlo.
    pub fn requeue(&self, id: i64) -> Result<()> {
        self.conn.lock().unwrap().execute(
            "UPDATE queue SET state = 'queued', taken_at = NULL WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    /// Rescata los items que quedaron en `running` sin nadie ejecutandolos.
    ///
    /// Pasa cuando el proceso que los tomo murio (kill, crash, o el `?` que
    /// tiraba abajo el daemon, N-08): `pick_next` solo mira `queued`, asi que
    /// esos items quedaban trabados para siempre. Los devuelve a la cola si no
    /// hay ningun run Running de ese dag+param. Devuelve cuantos rescato.
    pub fn requeue_orphans(&self) -> Result<usize> {
        let c = self.conn.lock().unwrap();
        let n = c.execute(
            "UPDATE queue SET state = 'queued', taken_at = NULL
             WHERE state = 'running'
               AND NOT EXISTS (
                   SELECT 1 FROM runs r
                   WHERE r.dag = queue.dag AND r.param = queue.param
                     AND r.state = 'Running'
               )",
            [],
        )?;
        Ok(n)
    }

    /// Marca `Failed` los runs que llevan demasiado tiempo en `Running` y que
    /// **nadie esta ejecutando**.
    ///
    /// El tope de tiempo del scheduler (`--timeout-secs`) solo alcanza a los
    /// runs que ese proceso lanzo y todavia tiene en la mano: mata el
    /// subproceso y cierra la fila. Un run que quedo huerfano —el scheduler se
    /// reinicio, o el proceso murio sin poder escribir— no lo agarra nadie, y
    /// se queda en `Running` para siempre.
    ///
    /// Eso no es solo cosmetico. `requeue_orphans` no devuelve a la cola un
    /// item mientras exista un run `Running` de ese dag+param, asi que la fila
    /// muerta y el item de la cola **se traban mutuamente**: el item queda
    /// tomado para siempre y ese dag+param no vuelve a correr nunca.
    ///
    /// `excepto` son los runs que el scheduler tiene vivos en este momento.
    /// Van afuera porque de esos ya se ocupa el tope normal, que ademas puede
    /// matar el proceso — cerrar aca la fila de un proceso que sigue vivo
    /// dejaria la base diciendo una cosa y la maquina haciendo otra.
    ///
    /// Devuelve los `(run_id, dag)` que cerro, para poder decirlo en el log.
    pub fn reap_stale_runs(&self, max_secs: i64, excepto: &[Uuid]) -> Result<Vec<(Uuid, String)>> {
        if max_secs <= 0 {
            return Ok(Vec::new());
        }
        let c = self.conn.lock().unwrap();
        let corte = Self::now() - max_secs;
        let vivos: Vec<String> = excepto.iter().map(|u| u.to_string()).collect();

        let mut stmt =
            c.prepare("SELECT id, dag FROM runs WHERE state = 'Running' AND started_at < ?1")?;
        let candidatos: Vec<(String, String)> = stmt
            .query_map(params![corte], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(stmt);

        let mut cerrados = Vec::new();
        for (id, dag) in candidatos {
            if vivos.contains(&id) {
                continue;
            }
            c.execute(
                "UPDATE runs SET state = 'Failed', finished_at = ?2
                 WHERE id = ?1 AND state = 'Running'",
                params![id, Self::now()],
            )?;
            // El item de la cola se libera en la misma pasada: si no, queda
            // 'running' hasta el proximo rescate y nadie entiende por que ese
            // DAG dejo de correr.
            c.execute(
                "DELETE FROM queue WHERE state = 'running' AND dag = ?1
                   AND param = (SELECT param FROM runs WHERE id = ?2)",
                params![dag, id],
            )?;
            if let Ok(u) = Uuid::parse_str(&id) {
                cerrados.push((u, dag));
            }
        }
        Ok(cerrados)
    }

    /// Saca de la cola un item, corriendo o no.
    ///
    /// Si ya tiene un run en curso pide cancelarlo (la señal viaja por la base
    /// y la levanta el scheduler en su proximo tick); si todavia no arranco,
    /// alcanza con borrar la fila. Devuelve el run que se mando a cancelar,
    /// si habia alguno.
    pub fn cancelar_item_cola(&self, id: i64) -> Result<Option<Uuid>> {
        let (dag, param, estado) = {
            let c = self.conn.lock().unwrap();
            c.query_row(
                "SELECT dag, param, state FROM queue WHERE id = ?1",
                params![id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| anyhow::anyhow!("no hay un item {id} en la cola"))?
        };

        let run = if estado == "running" {
            let c = self.conn.lock().unwrap();
            c.query_row(
                "SELECT id FROM runs WHERE dag = ?1 AND param = ?2 AND state = 'Running'
                 ORDER BY started_at DESC LIMIT 1",
                params![dag, param],
                |r| r.get::<_, String>(0),
            )
            .optional()?
            .and_then(|s| Uuid::parse_str(&s).ok())
        } else {
            None
        };

        if let Some(u) = run {
            self.request_cancel(u);
        }
        self.conn
            .lock()
            .unwrap()
            .execute("DELETE FROM queue WHERE id = ?1", params![id])?;
        Ok(run)
    }
}

fn clave_pausa(dag: &str) -> String {
    format!("pausa:{dag}")
}

/// Cuantos dias se guarda el DETALLE por tarea (`task_runs`). Es lo que mas
/// pesa de la base y lo que menos se mira pasada la primera semana.
pub const RETENCION_DETALLE_DIAS: i64 = 30;

/// Cuantos dias se guardan las corridas (`runs`). Al borrarse, su volumen queda
/// igual en `rollups_monthly`, que no se purga nunca.
pub const RETENCION_RUNS_DIAS: i64 = 365;

/// Nombre que tenia la base antes de que el proyecto se llamara zeruel.
const BASE_LEGADA: &str = "nodo.db";

/// Avisa si en el directorio de estado conviven dos bases.
///
/// El nombre del archivo es parte del contrato entre el scheduler y los
/// pipelines, aunque no lo parezca: **cada pipeline lleva su propia copia de
/// esta libreria compilada adentro**, asi que el que se compilo antes del
/// cambio de nombre abre `nodo.db` mientras el scheduler abre `zeruel.db`.
///
/// El sintoma es de los peores que hay: el pipeline corre bien, escribe su
/// resultado, y el scheduler —mirando la otra base— ve el run sin cerrar y lo
/// marca **fallido**. Un run exitoso reportado como fallo, sin ningun error que
/// lo explique. Se descubrio exactamente asi.
///
/// No hay alias posible: el pipeline decide el nombre desde su propia copia de
/// la libreria, y ya esta compilado. La unica salida es recompilarlo, asi que
/// lo que corresponde es decirlo fuerte y con el remedio incluido.
fn avisar_base_partida(dir: &Path) {
    if !dir.join(BASE_LEGADA).exists() {
        return;
    }
    tracing::error!(
        "hay dos bases en {}: `{BASE_LEGADA}` y `zeruel.db`.
         Algun pipeline se compilo antes del cambio de nombre y escribe en la          vieja, mientras el scheduler lee la nueva. Sus runs van a terminar bien          y aparecer como FALLIDOS.
         Se arregla recompilando los pipelines y volviendo a copiarlos al          directorio de despliegue. Despues se puede borrar `{BASE_LEGADA}`.",
        dir.display()
    );
}
#[cfg(test)]
mod tests {

    #[test]
    fn una_alerta_cerrada_no_vuelve_pero_un_fallo_nuevo_si() {
        // Es la regla que hace que cerrar alertas sirva de algo. Cerrar una
        // corrida fallida vale para siempre —una corrida es inmutable— pero si
        // el DAG vuelve a fallar, esa es OTRA corrida y tiene que avisar.
        let s = Store::open_in_memory().unwrap();
        let primera = s.create_run("d", "hoy").unwrap();
        s.set_run_state(primera, RunState::Failed).unwrap();

        assert!(s.alerta_vista(&primera.to_string()).is_none());
        s.cerrar_alerta(&primera.to_string(), 1_000).unwrap();
        assert!(s.alerta_vista(&primera.to_string()).is_some());

        // Otra corrida del mismo DAG que tambien falla: no esta cerrada.
        let segunda = s.create_run("d", "manana").unwrap();
        s.set_run_state(segunda, RunState::Failed).unwrap();
        assert!(
            s.alerta_vista(&segunda.to_string()).is_none(),
            "cerrar una corrida no puede silenciar las que fallen despues"
        );
    }

    #[test]
    fn las_marcas_viejas_se_purgan() {
        let s = Store::open_in_memory().unwrap();
        s.cerrar_alerta("vieja", 1_000).unwrap();
        s.cerrar_alerta("nueva", 9_000).unwrap();

        let n = s.purgar_alertas_vistas(5_000).unwrap();
        assert_eq!(n, 1);
        assert!(s.alerta_vista("vieja").is_none());
        assert!(s.alerta_vista("nueva").is_some(), "la reciente se queda");
    }

    #[test]
    fn pausar_no_frena_lo_que_ya_esta_en_marcha() {
        // Pausar es "deja de agendarlo", no "mata lo que esta haciendo". Si
        // matara, nadie se animaria a usarlo sobre un ETL de dos horas.
        let s = Store::open_in_memory().unwrap();
        s.enqueue("d", "hoy", 0).unwrap();
        let run = s.create_run("d", "hoy").unwrap();

        s.pausar_dag("d").unwrap();

        assert!(s.dag_pausado("d"));
        assert_eq!(s.queue_items().unwrap().len(), 1, "lo encolado sigue");
        assert_eq!(
            s.run_by_id(run).unwrap().unwrap().state,
            RunState::Running,
            "lo que corre sigue corriendo"
        );
    }

    #[test]
    fn activar_no_deja_basura_en_meta() {
        // Se borra la clave en vez de guardar "0": con 56 DAGs, guardar el
        // estado de todos los que alguna vez se pausaron llena la tabla de
        // filas que solo dicen "no".
        let s = Store::open_in_memory().unwrap();
        s.pausar_dag("d").unwrap();
        assert_eq!(s.dags_pausados().unwrap(), vec!["d".to_string()]);

        s.activar_dag("d").unwrap();
        assert!(!s.dag_pausado("d"));
        assert!(s.dags_pausados().unwrap().is_empty());
    }

    #[test]
    fn un_dag_sin_tocar_no_esta_pausado() {
        let s = Store::open_in_memory().unwrap();
        assert!(!s.dag_pausado("nunca_visto"));
        assert!(s.dags_pausados().unwrap().is_empty());
    }

    #[test]
    fn el_filtro_por_dag_no_se_pierde_detras_del_limite() {
        // El caso real: un DAG que corrio hace rato, tapado por otros que
        // corren seguido. Antes se traian las ultimas N de TODOS y se filtraba
        // sobre eso, asi que este DAG devolvia una lista vacia y la pantalla
        // decia que nunca habia corrido.
        let s = Store::open_in_memory().unwrap();
        let viejo = s.create_run("el_que_importa", "ayer").unwrap();
        s.set_run_state(viejo, RunState::Failed).unwrap();

        // 150 corridas de otro DAG, todas posteriores: tapan al de arriba.
        for i in 0..150 {
            s.create_run("ruidoso", &format!("p{i}")).unwrap();
        }

        let con_filtro = s.recent_runs_de(Some("el_que_importa"), 100, 0).unwrap();
        assert_eq!(
            con_filtro.len(),
            1,
            "el DAG tapado tiene que aparecer igual: el filtro va en el SQL"
        );
        assert_eq!(con_filtro[0].id, viejo);
        assert_eq!(con_filtro[0].state, RunState::Failed);

        // Y sin filtro sigue devolviendo lo de siempre, acotado por el limite.
        assert_eq!(s.recent_runs(100, 0).unwrap().len(), 100);
    }

    #[test]
    fn el_filtro_respeta_limite_y_offset() {
        let s = Store::open_in_memory().unwrap();
        for i in 0..10 {
            s.create_run("a", &format!("p{i}")).unwrap();
            s.create_run("b", &format!("p{i}")).unwrap();
        }
        // Solo las de `a`, no las intercaladas de `b`.
        let pag = s.recent_runs_de(Some("a"), 3, 2).unwrap();
        assert_eq!(pag.len(), 3);
        assert!(pag.iter().all(|r| r.dag == "a"));
    }

    #[test]
    fn detecta_la_base_partida_por_el_cambio_de_nombre() {
        let tmp = tempfile::tempdir().unwrap();
        // Sin la base vieja no hay nada que avisar.
        assert!(!tmp.path().join(BASE_LEGADA).exists());
        let s = Store::open(tmp.path()).unwrap();
        drop(s);

        // Con la base vieja al lado, abrir tiene que seguir funcionando: el
        // aviso informa, no bloquea. Que el scheduler se niegue a arrancar
        // seria peor que un run mal reportado.
        std::fs::write(tmp.path().join(BASE_LEGADA), b"").unwrap();
        let s = Store::open(tmp.path());
        assert!(s.is_ok(), "el aviso no puede impedir abrir la base");
    }
    use super::*;
    use zeruel_sdk::TaskState;

    fn store() -> Store {
        Store::open_in_memory().unwrap()
    }

    /// El mismo "ahora" que usa el Store.
    fn ahora() -> i64 {
        chrono::Utc::now().timestamp()
    }

    /// Inserta un run con estado y fecha a medida (para probar GC y filtros).
    fn run_con(s: &Store, dag: &str, param: &str, estado: &str, started_at: i64) -> Uuid {
        let id = Uuid::new_v4();
        s.conn
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO runs (id, dag, param, state, started_at, finished_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
                params![id.to_string(), dag, param, estado, started_at],
            )
            .unwrap();
        id
    }

    // ---- runs ----

    #[test]
    fn cancelled_no_se_lee_como_running() {
        // N-01: el `_ => Running` del match hacia invisible a Cancelled.
        let s = store();
        let id = s.create_run("d", "p").unwrap();
        assert!(s.request_cancel(id), "el run estaba Running, se cancela");
        let r = s.run_by_id(id).unwrap().expect("existe");
        assert_eq!(r.state, RunState::Cancelled);
        assert_eq!(s.recent_runs(10, 0).unwrap()[0].state, RunState::Cancelled);
    }

    #[test]
    fn cancelled_no_lo_pisa_el_final_del_run() {
        // N-02: el engine termina su ultima task y hace set_run_state(Success);
        // eso no puede resucitar un run que ya fue cancelado.
        let s = store();
        let id = s.create_run("d", "p").unwrap();
        s.request_cancel(id);
        s.set_run_state(id, RunState::Success).unwrap();
        assert_eq!(s.run_by_id(id).unwrap().unwrap().state, RunState::Cancelled);
    }

    #[test]
    fn cancel_se_ve_desde_otra_conexion() {
        // N-02: el pedido viaja por la DB, no solo por el flag en memoria.
        let tmp = tempfile::tempdir().unwrap();
        let a = Store::open(tmp.path()).unwrap();
        let id = a.create_run("d", "p").unwrap();
        let b = Store::open(tmp.path()).unwrap(); // "otro proceso"
        assert!(!b.cancel_requested(id));
        b.request_cancel(id);
        assert!(
            a.cancel_requested(id),
            "el proceso que corre el run lo tiene que ver"
        );
        a.clear_cancel(id);
        assert!(
            !a.cancel_requested(id),
            "clear_cancel no deja basura en meta"
        );
    }

    #[test]
    fn run_by_id_trae_el_run_pedido_no_el_ultimo() {
        // N-03 / N-13.
        let s = store();
        let viejo = s.create_run("d", "P1").unwrap();
        for i in 0..30 {
            s.create_run("d", &format!("otro{i}")).unwrap();
        }
        let r = s
            .run_by_id(viejo)
            .unwrap()
            .expect("el run viejo sigue existiendo");
        assert_eq!(r.id, viejo);
        assert_eq!(r.param, "P1");
        assert!(
            s.run_by_id(Uuid::new_v4()).unwrap().is_none(),
            "id inexistente = None"
        );
    }

    #[test]
    fn recover_running_no_cruza_params() {
        // N-06: un run colgado de P1 no se puede retomar corriendo P2.
        let s = store();
        let p1 = s.create_run("d", "P1").unwrap(); // queda Running
        assert_eq!(s.recover_running("d", "P1").unwrap(), Some(p1));
        assert_eq!(s.recover_running("d", "P2").unwrap(), None);
        assert_eq!(s.recover_running("otro", "P1").unwrap(), None);
    }

    // ---- task_runs ----

    #[test]
    fn task_reutilizada_no_ensucia_las_stats() {
        // N-07: las filas `reused` no son mediciones de ese run.
        let s = store();
        let r1 = s.create_run("d", "p").unwrap();
        s.set_task_state(TaskStateRecord::medido(
            r1,
            "validar",
            &TaskState::Success,
            1000,
            "ok",
            500,
            2.0,
        ))
        .unwrap();
        let r2 = s.create_run("d", "p").unwrap();
        s.set_task_state(TaskStateRecord {
            reused: true,
            ..TaskStateRecord::medido(r2, "validar", &TaskState::Success, 0, "reutilizado", 0, 0.0)
        })
        .unwrap();

        let stats = s.task_stats().unwrap();
        let v = stats.iter().find(|t| t.task == "validar").unwrap();
        assert_eq!(v.ejecuciones, 1, "la reutilizada no cuenta como ejecucion");
        assert_eq!(v.filas_prom, 1000.0, "el promedio no se hunde a la mitad");

        let (rows, ram, cpu) = s.last_success_metrics("d", "p", "validar").unwrap();
        assert_eq!((rows, ram), (1000, 500));
        assert_eq!(cpu, 2.0);
    }

    // ---- cola ----

    #[test]
    fn enqueue_rechaza_duplicado_y_run_activo() {
        let s = store();
        assert!(s.enqueue("d", "p", 0).unwrap().ok);
        let dup = s.enqueue("d", "p", 0).unwrap();
        assert!(!dup.ok);
        assert!(dup.motivo.contains("en cola"));
        // mismo DAG, otro param: entra
        assert!(s.enqueue("d", "otro", 0).unwrap().ok);
        // con un run activo del mismo dag+param: rechaza
        s.create_run("d", "activo").unwrap();
        let r = s.enqueue("d", "activo", 0).unwrap();
        assert!(!r.ok);
        assert!(r.motivo.contains("activo"));
    }

    #[test]
    fn cola_acotada_rechaza_cuando_esta_llena() {
        let s = store();
        s.meta_set("queue.max_size", "2").unwrap();
        assert!(s.enqueue("d", "a", 0).unwrap().ok);
        assert!(s.enqueue("d", "b", 0).unwrap().ok);
        let r = s.enqueue("d", "c", 0).unwrap();
        assert!(!r.ok);
        assert!(
            r.motivo.contains("cola llena"),
            "motivo claro: {}",
            r.motivo
        );
        // sin tope vuelve a entrar
        s.meta_set("queue.max_size", "0").unwrap();
        assert!(s.enqueue("d", "c", 0).unwrap().ok);
    }

    #[test]
    fn pick_next_ordena_por_prioridad_y_despues_fifo() {
        let s = store();
        s.enqueue("d", "viejo_prio0", 0).unwrap();
        s.enqueue("d", "prio5", 5).unwrap();
        s.enqueue("d", "prio1", 1).unwrap();
        let p = s.pick_next(&[], 100_000, 300, 20, 0).unwrap();
        assert_eq!(
            p.lanzado.unwrap().param,
            "prio5",
            "gana la prioridad mas alta"
        );
        let p = s.pick_next(&[], 100_000, 300, 20, 0).unwrap();
        assert_eq!(p.lanzado.unwrap().param, "prio1");
    }

    #[test]
    fn pick_next_respeta_el_conflicto_por_dag_param() {
        let s = store();
        s.enqueue("d", "p", 0).unwrap();
        let p = s
            .pick_next(&["d|p".to_string()], 100_000, 300, 20, 0)
            .unwrap();
        assert!(p.lanzado.is_none());
        assert_eq!(p.bloqueados_conflicto, 1);
    }

    /// Una sola medicion absurda NO puede bloquear un DAG para siempre.
    ///
    /// Es el caso real del 31/08/2026: un bug atribuyo a la tarea el pico de
    /// memoria de la maquina entera y dejo a `consolidado` con 16866 MB
    /// grabados. El control de admision le pedia 20.239 MB, la maquina tenia
    /// 17.592 libres, y como nunca lograba correr nunca volvia a medir bien:
    /// el bloqueo se sostenia a si mismo. Ocho items estuvieron 1,5 horas en
    /// cola, y el scheduler no decia nada.
    #[test]
    fn un_pico_mal_medido_no_bloquea_al_dag_para_siempre() {
        let s = store();
        // 40 corridas normales de ~450 MB, y una sola disparatada.
        for i in 0..40 {
            let r = s.create_run("consolidado", &format!("p{i}")).unwrap();
            s.set_task_state(TaskStateRecord::medido(
                r,
                "t",
                &TaskState::Success,
                1,
                "ok",
                450,
                1.0,
            ))
            .unwrap();
            s.set_run_state(r, RunState::Success).unwrap();
        }
        let malo = s.create_run("consolidado", "el-31-de-agosto").unwrap();
        s.set_task_state(TaskStateRecord::medido(
            malo,
            "t",
            &TaskState::Success,
            1,
            "ok",
            16866, // el pico de la maquina, no el de la tarea
            1.0,
        ))
        .unwrap();
        s.set_run_state(malo, RunState::Success).unwrap();

        s.enqueue("consolidado", "hoy", 0).unwrap();

        // Con 17.592 MB libres -los que habia de verdad- tiene que lanzar.
        let p = s.pick_next(&[], 17_592, 300, 20, 0).unwrap();
        assert!(
            p.lanzado.is_some(),
            "un solo pico mal medido dejo el DAG bloqueado otra vez"
        );
    }

    /// El caso de `reporte_mensual`: pocas corridas y una medicion absurda.
    ///
    /// Con 13 mediciones el 5% mas alto es CERO filas, asi que el p95 solo no
    /// descarta nada y el valor malo se queda. Por eso ademas se acota contra
    /// la mediana, que unos pocos disparates no mueven.
    #[test]
    fn con_pocas_corridas_un_solo_disparate_tampoco_bloquea() {
        let s = store();
        for i in 0..12 {
            let r = s.create_run("reporte_mensual", &format!("p{i}")).unwrap();
            s.set_task_state(TaskStateRecord::medido(
                r,
                "correr",
                &TaskState::Success,
                1,
                "ok",
                240,
                1.0,
            ))
            .unwrap();
            s.set_run_state(r, RunState::Success).unwrap();
        }
        let malo = s.create_run("reporte_mensual", "el-31").unwrap();
        s.set_task_state(TaskStateRecord::medido(
            malo,
            "correr",
            &TaskState::Success,
            1,
            "ok",
            16863,
            1.0,
        ))
        .unwrap();
        s.set_run_state(malo, RunState::Success).unwrap();

        s.enqueue("reporte_mensual", "hoy", 0).unwrap();
        let p = s.pick_next(&[], 17_592, 300, 20, 0).unwrap();
        assert!(
            p.lanzado.is_some(),
            "con 13 mediciones el p95 no descarta nada: hace falta la mediana"
        );
    }

    /// La cota no puede aplastar a un DAG que de verdad usa mucha memoria: si
    /// TODAS sus mediciones son grandes, la mediana tambien lo es.
    #[test]
    fn un_dag_que_realmente_es_pesado_sigue_siendo_pesado() {
        let s = store();
        for i in 0..30 {
            let r = s.create_run("pesado", &format!("p{i}")).unwrap();
            s.set_task_state(TaskStateRecord::medido(
                r,
                "t",
                &TaskState::Success,
                1,
                "ok",
                4000,
                1.0,
            ))
            .unwrap();
            s.set_run_state(r, RunState::Success).unwrap();
        }
        s.enqueue("pesado", "hoy", 0).unwrap();

        // 4000 + 20% = 4800: no entra en 4000, si entra en 5000.
        assert!(s
            .pick_next(&[], 4000, 300, 20, 0)
            .unwrap()
            .lanzado
            .is_none());
        assert!(s
            .pick_next(&[], 5000, 300, 20, 0)
            .unwrap()
            .lanzado
            .is_some());
    }

    /// Postergar por memoria tiene PLAZO. Un item que espera para siempre no
    /// protege nada: no corre, no falla, no avisa y no libera su lugar, y desde
    /// afuera se ve igual que un sistema sano con trabajo pendiente.
    ///
    /// Pasado el plazo se lanza. Si de verdad se excede, lo corta `ram_max` —
    /// que es el que protege la maquina— y queda un diagnostico con cuanto uso
    /// y cual era el limite.
    #[test]
    fn lo_que_espero_demasiado_se_lanza_igual_aunque_no_entre() {
        let s = store();
        let r = s.create_run("pesado", "viejo").unwrap();
        s.set_task_state(TaskStateRecord::medido(
            r,
            "t",
            &TaskState::Success,
            1,
            "ok",
            5000,
            1.0,
        ))
        .unwrap();
        s.set_run_state(r, RunState::Success).unwrap();
        s.enqueue("pesado", "nuevo", 0).unwrap();

        // Recien encolado y sin lugar: espera, como corresponde.
        let p = s.pick_next(&[], 1000, 300, 20, 600).unwrap();
        assert!(p.lanzado.is_none(), "recien encolado tiene que esperar");
        assert_eq!(p.no_caben_ram, 1);

        // Se lo envejece mas alla del plazo.
        s.conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE queue SET enqueued_at = enqueued_at - 1200",
                rusqlite::params![],
            )
            .unwrap();

        let p = s.pick_next(&[], 1000, 300, 20, 600).unwrap();
        assert!(
            p.lanzado.is_some(),
            "pasado el plazo tiene que lanzarse igual, no esperar para siempre"
        );
        let (pedia, habia, esperando) = p.forzado_por_espera.expect("tiene que quedar dicho");
        assert_eq!(pedia, 6000, "5000 de estimacion + 20%");
        assert_eq!(habia, 1000);
        assert!(esperando >= 1200);
    }

    /// Con el plazo en 0 se posterga indefinidamente: es el comportamiento
    /// anterior, y sigue disponible para quien lo quiera.
    #[test]
    fn sin_plazo_se_posterga_como_antes() {
        let s = store();
        let r = s.create_run("pesado", "viejo").unwrap();
        s.set_task_state(TaskStateRecord::medido(
            r,
            "t",
            &TaskState::Success,
            1,
            "ok",
            5000,
            1.0,
        ))
        .unwrap();
        s.set_run_state(r, RunState::Success).unwrap();
        s.enqueue("pesado", "nuevo", 0).unwrap();
        s.conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE queue SET enqueued_at = enqueued_at - 100000",
                rusqlite::params![],
            )
            .unwrap();

        let p = s.pick_next(&[], 1000, 300, 20, 0).unwrap();
        assert!(p.lanzado.is_none());
        assert!(p.forzado_por_espera.is_none());
    }

    /// Cuando SI falta memoria hay que decir cual, cuanto pide y cuanto hay.
    /// Contar los que no entran no alcanza para hacer nada al respecto.
    #[test]
    fn cuando_no_entra_se_dice_cual_y_con_que_numeros() {
        let s = store();
        let r = s.create_run("pesado", "viejo").unwrap();
        s.set_task_state(TaskStateRecord::medido(
            r,
            "t",
            &TaskState::Success,
            1,
            "ok",
            1000,
            1.0,
        ))
        .unwrap();
        s.set_run_state(r, RunState::Success).unwrap();
        s.enqueue("pesado", "nuevo", 0).unwrap();

        let p = s.pick_next(&[], 1100, 300, 20, 0).unwrap();
        assert!(p.lanzado.is_none());
        let (dag, necesita, hay) = p.primero_sin_ram.expect("tiene que decir cual fue");
        assert_eq!(dag, "pesado");
        assert_eq!(necesita, 1200, "1000 de pico + 20%");
        assert_eq!(hay, 1100);
    }

    #[test]
    fn pick_next_salta_lo_que_no_entra_en_ram() {
        // N-23: la cuenta es peak * (1 + margen) > disponible.
        let s = store();
        let r = s.create_run("pesado", "viejo").unwrap();
        s.set_task_state(TaskStateRecord::medido(
            r,
            "t",
            &TaskState::Success,
            1,
            "ok",
            1000,
            1.0,
        ))
        .unwrap();
        s.set_run_state(r, RunState::Success).unwrap();
        s.enqueue("pesado", "nuevo", 0).unwrap();

        // 1000 MB de pico + 20% = 1200: no entra en 1100, si entra en 1500
        let p = s.pick_next(&[], 1100, 300, 20, 0).unwrap();
        assert!(p.lanzado.is_none());
        assert_eq!(p.no_caben_ram, 1);
        let p = s.pick_next(&[], 1500, 300, 20, 0).unwrap();
        assert!(p.lanzado.is_some());
    }

    #[test]
    fn aging_evita_que_un_prio_bajo_espere_para_siempre() {
        let s = store();
        s.enqueue("d", "nuevo_alto", 5).unwrap();
        s.enqueue("d", "viejo_bajo", 0).unwrap();
        // el viejo espera 1 hora: +12 puntos de aging con ventana de 300s
        s.conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE queue SET enqueued_at = enqueued_at - 3600 WHERE param = 'viejo_bajo'",
                [],
            )
            .unwrap();
        let p = s.pick_next(&[], 100_000, 300, 20, 0).unwrap();
        assert_eq!(
            p.lanzado.unwrap().param,
            "viejo_bajo",
            "el aging lo pasa adelante"
        );
    }

    #[test]
    fn requeue_orphans_rescata_items_trabados() {
        // N-08 / N-41: el proceso que tomo el item murio.
        let s = store();
        s.enqueue("d", "p", 0).unwrap();
        let p = s.pick_next(&[], 100_000, 300, 20, 0).unwrap();
        let id = p.lanzado.unwrap().id;
        assert!(s
            .pick_next(&[], 100_000, 300, 20, 0)
            .unwrap()
            .lanzado
            .is_none());
        assert_eq!(s.requeue_orphans().unwrap(), 1);
        let p = s.pick_next(&[], 100_000, 300, 20, 0).unwrap();
        assert_eq!(p.lanzado.unwrap().id, id, "vuelve a estar disponible");
        // si hay un run Running de ese dag+param, no lo toca (lo esta corriendo alguien)
        s.create_run("d", "p").unwrap();
        assert_eq!(s.requeue_orphans().unwrap(), 0);
    }

    // ---- higiene / GC ----

    #[test]
    fn gc_consolida_los_meses_cerrados_sin_esperar_un_ano() {
        // N-40: antes rollups_monthly solo se llenaba con runs de +1 año.
        let s = store();
        let hace_45_dias = ahora() - 45 * 86_400;
        run_con(&s, "d", "p1", "Success", hace_45_dias);
        run_con(&s, "d", "p2", "Failed", hace_45_dias);
        let rep = s.gc(false).unwrap();
        assert!(rep.meses_agregados > 0, "el mes cerrado se agrega");
        let roll = s.rollups().unwrap();
        let m = roll
            .iter()
            .find(|r| r.dag == "d")
            .expect("hay agregado del dag");
        assert_eq!((m.runs, m.ok, m.fail), (2, 1, 1));
        // idempotente: correrla de nuevo no duplica
        s.gc(false).unwrap();
        let m2 = s
            .rollups()
            .unwrap()
            .into_iter()
            .find(|r| r.dag == "d")
            .unwrap();
        assert_eq!(m2.runs, 2, "recalcula, no acumula");
    }

    #[test]
    fn gc_borra_detalle_viejo_y_runs_muy_viejos() {
        let s = store();
        let t = ahora();
        let viejo = run_con(&s, "d", "viejo", "Success", t - 400 * 86_400);
        let medio = run_con(&s, "d", "medio", "Success", t - 45 * 86_400);
        let nuevo = run_con(&s, "d", "nuevo", "Success", t - 86_400);
        for id in [viejo, medio, nuevo] {
            s.set_task_state(TaskStateRecord::medido(
                id,
                "t",
                &TaskState::Success,
                1,
                "ok",
                1,
                1.0,
            ))
            .unwrap();
        }
        let rep = s.gc(false).unwrap();
        assert_eq!(rep.runs_borrados, 1, "solo el de +1 año");
        assert_eq!(rep.task_runs_borrados, 2, "el detalle de los de +30d");
        assert!(s.run_by_id(nuevo).unwrap().is_some());
        assert!(
            s.run_by_id(medio).unwrap().is_some(),
            "el run queda, se va su detalle"
        );
        assert!(s.run_by_id(viejo).unwrap().is_none());
        assert!(s.hygiene().unwrap().ultima_gc.is_some());
    }

    #[test]
    fn gc_dry_run_no_borra_nada() {
        let s = store();
        run_con(&s, "d", "viejo", "Success", ahora() - 400 * 86_400);
        let rep = s.gc(true).unwrap();
        assert_eq!(rep.runs_borrados, 1, "cuenta lo que borraria");
        assert_eq!(s.hygiene().unwrap().runs, 1, "pero no lo borra");
    }

    // ---- meta / dag_stats ----

    #[test]
    fn meta_set_get_del() {
        let s = store();
        assert_eq!(s.meta_get("k").unwrap(), None);
        s.meta_set("k", "v").unwrap();
        assert_eq!(s.meta_get("k").unwrap().as_deref(), Some("v"));
        s.meta_set("k", "v2").unwrap();
        assert_eq!(s.meta_get("k").unwrap().as_deref(), Some("v2"));
        assert!(s.meta_del("k").unwrap());
        assert_eq!(s.meta_get("k").unwrap(), None);
        assert!(!s.meta_del("k").unwrap(), "borrar lo que no esta no falla");
    }

    #[test]
    fn dag_stats_cuenta_cada_estado_por_separado() {
        let s = store();
        let t = ahora();
        run_con(&s, "a", "1", "Success", t);
        run_con(&s, "a", "2", "Failed", t);
        run_con(&s, "a", "3", "Cancelled", t);
        run_con(&s, "a", "4", "Running", t);
        run_con(&s, "b", "1", "Success", t);
        let stats = s.dag_stats().unwrap();
        let a = stats.iter().find(|d| d.dag == "a").unwrap();
        assert_eq!(
            (a.runs, a.ok, a.fail, a.cancelled, a.running),
            (4, 1, 1, 1, 1)
        );
        assert_eq!(stats.len(), 2);
    }

    #[test]
    fn failed_since_filtra_por_fecha() {
        let s = store();
        let t = ahora();
        run_con(&s, "d", "hoy", "Failed", t - 3600);
        run_con(&s, "d", "anteayer", "Failed", t - 3 * 86_400);
        run_con(&s, "d", "ok", "Success", t);
        let f = s.failed_since(t - 86_400).unwrap();
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].param, "hoy");
    }

    #[test]
    fn hygiene_suma_el_wal_al_tamano() {
        // N-39: en WAL lo recien escrito no esta en el .db todavia.
        let tmp = tempfile::tempdir().unwrap();
        let s = Store::open(tmp.path()).unwrap();
        for i in 0..50 {
            let id = s.create_run("d", &format!("p{i}")).unwrap();
            s.set_task_state(TaskStateRecord::medido(
                id,
                "t",
                &TaskState::Success,
                1,
                "ok",
                1,
                1.0,
            ))
            .unwrap();
        }
        let h = s.hygiene().unwrap();
        let solo_db = tmp.path().join("zeruel.db").metadata().unwrap().len();
        assert!(
            h.db_size_bytes >= solo_db,
            "no puede ser menor que el .db solo"
        );
        assert!(h.db_size_bytes > 0);
    }
}

#[cfg(test)]
mod tests_concurrencia {
    use super::*;
    use std::sync::Arc;
    use zeruel_sdk::TaskState;

    /// N-57: varios "procesos" (conexiones independientes al mismo archivo)
    /// escribiendo checkpoints a la vez. Sin `busy_timeout` esto falla con
    /// "database is locked"; es exactamente lo que hace el scheduler
    /// multiproceso cuando lanza varios pipelines.
    #[test]
    fn muchos_escritores_sobre_la_misma_base() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();

        // Un run por "proceso", creado desde una conexion aparte.
        let inicial = Store::open(&dir).unwrap();
        let ids: Vec<_> = (0..6)
            .map(|i| inicial.create_run("d", &format!("p{i}")).unwrap())
            .collect();
        drop(inicial);

        let errores: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let mut hs = Vec::new();
        for (i, run) in ids.into_iter().enumerate() {
            let dir = dir.clone();
            let errores = Arc::clone(&errores);
            hs.push(std::thread::spawn(move || {
                // Cada thread abre su PROPIA conexion: es lo que hace un proceso.
                let s = match Store::open(&dir) {
                    Ok(s) => s,
                    Err(e) => {
                        errores.lock().unwrap().push(format!("abrir {i}: {e}"));
                        return;
                    }
                };
                for t in 0..40 {
                    let tarea = format!("t{t}");
                    let r = s.set_task_state(TaskStateRecord::medido(
                        run,
                        &tarea,
                        &TaskState::Success,
                        t as u64,
                        "ok",
                        1,
                        0.1,
                    ));
                    if let Err(e) = r {
                        errores
                            .lock()
                            .unwrap()
                            .push(format!("escribir {i}/{t}: {e}"));
                        return;
                    }
                }
                if let Err(e) = s.set_run_state(run, RunState::Success) {
                    errores.lock().unwrap().push(format!("cerrar {i}: {e}"));
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }

        let errs = errores.lock().unwrap();
        assert!(
            errs.is_empty(),
            "escrituras concurrentes fallaron: {errs:?}"
        );

        // Y todo quedo escrito: 6 runs x 40 tareas.
        let s = Store::open(&dir).unwrap();
        assert_eq!(s.hygiene().unwrap().task_runs, 240);
        assert_eq!(
            s.recent_runs(10, 0)
                .unwrap()
                .iter()
                .filter(|r| r.state == RunState::Success)
                .count(),
            6
        );
    }
}

#[cfg(test)]
mod tests_colgados {
    use crate::store::{RunState, Store};

    fn store() -> Store {
        Store::open_in_memory().unwrap()
    }

    /// Envejece un run y su item de cola, para simular que quedaron colgados.
    fn envejecer(s: &Store, segundos: i64) {
        let c = s.conn.lock().unwrap();
        c.execute(
            "UPDATE runs SET started_at = started_at - ?1",
            rusqlite::params![segundos],
        )
        .unwrap();
        c.execute(
            "UPDATE queue SET enqueued_at = enqueued_at - ?1, taken_at = taken_at - ?1",
            rusqlite::params![segundos],
        )
        .unwrap();
    }

    /// El caso que se vio en produccion: un run que quedo en `Running` sin
    /// nadie ejecutandolo. El tope del scheduler no lo alcanza porque solo
    /// mira los subprocesos que ese proceso tiene en la mano.
    #[test]
    fn un_run_colgado_pasa_a_fallido() {
        let s = store();
        let r = s.create_run("carga_sur", "p").unwrap();
        envejecer(&s, 20 * 3600); // 20 horas

        let cerrados = s.reap_stale_runs(10_800, &[]).unwrap();
        assert_eq!(cerrados.len(), 1, "tiene que cerrarlo: {cerrados:?}");
        assert_eq!(cerrados[0].0, r);
        assert_eq!(cerrados[0].1, "carga_sur");

        let run = s.run_by_id(r).unwrap().unwrap();
        assert!(
            matches!(run.state, RunState::Failed),
            "queda fallido, no Running: {:?}",
            run.state
        );
        assert!(run.finished_at.is_some(), "y con hora de cierre");
    }

    /// Lo que el scheduler tiene vivo NO se toca: de eso ya se ocupa el tope
    /// normal, que ademas mata el subproceso. Cerrar aca la fila de un proceso
    /// que sigue andando dejaria la base diciendo una cosa y la maquina otra.
    #[test]
    fn no_toca_lo_que_el_scheduler_esta_ejecutando() {
        let s = store();
        let r = s.create_run("d", "p").unwrap();
        envejecer(&s, 20 * 3600);

        let cerrados = s.reap_stale_runs(10_800, &[r]).unwrap();
        assert!(cerrados.is_empty(), "estaba vivo: {cerrados:?}");
        let run = s.run_by_id(r).unwrap().unwrap();
        assert!(matches!(run.state, RunState::Running));
    }

    /// Un run reciente no se toca aunque no lo ejecute nadie: recien arranco.
    #[test]
    fn un_run_reciente_se_deja_en_paz() {
        let s = store();
        let r = s.create_run("d", "p").unwrap();
        let cerrados = s.reap_stale_runs(10_800, &[]).unwrap();
        assert!(cerrados.is_empty());
        assert!(matches!(
            s.run_by_id(r).unwrap().unwrap().state,
            RunState::Running
        ));
    }

    /// Sin tope configurado no se cierra nada: `timeout_secs = 0` significa
    /// "sin tope" en toda la CLI y aca tiene que significar lo mismo.
    #[test]
    fn sin_tope_no_cierra_nada() {
        let s = store();
        s.create_run("d", "p").unwrap();
        envejecer(&s, 1000 * 3600);
        assert!(s.reap_stale_runs(0, &[]).unwrap().is_empty());
    }

    /// El trabon que dejaba items en la cola 56 horas: `requeue_orphans` no
    /// rescata un item mientras exista un run `Running` de ese dag+param, asi
    /// que la fila muerta protegia al item y ninguno de los dos se movia.
    /// Cerrando el run primero, la cola se destraba.
    #[test]
    fn el_run_muerto_dejaba_trabado_al_item_de_la_cola() {
        let s = store();
        s.enqueue("carga_sur", "2026-08-25T13:00", 0).unwrap();
        let r = s.create_run("carga_sur", "2026-08-25T13:00").unwrap();
        {
            let c = s.conn.lock().unwrap();
            c.execute("UPDATE queue SET state = 'running', taken_at = 1", [])
                .unwrap();
        }
        envejecer(&s, 56 * 3600);

        // Antes del arreglo: el rescate no puede hacer nada.
        assert_eq!(
            s.requeue_orphans().unwrap(),
            0,
            "el run Running lo bloquea, que es el trabon"
        );

        // Cerrando el run colgado, la cola queda libre.
        assert_eq!(s.reap_stale_runs(10_800, &[]).unwrap().len(), 1);
        let quedan = s.queue_items().unwrap();
        assert!(
            quedan.is_empty(),
            "el item tiene que salir de la cola: {quedan:?}"
        );
        assert!(matches!(
            s.run_by_id(r).unwrap().unwrap().state,
            RunState::Failed
        ));
    }

    /// Un item que TODAVIA NO ARRANCO no tiene run, asi que el boton de la
    /// tabla de corridas —que va por UUID— no lo alcanza. Desde la cola si.
    #[test]
    fn se_puede_sacar_un_item_que_no_arranco() {
        let s = store();
        s.enqueue("d", "p", 0).unwrap();
        let id = s.queue_items().unwrap()[0].id;

        let run = s.cancelar_item_cola(id).unwrap();
        assert!(run.is_none(), "no habia run que cancelar");
        assert!(s.queue_items().unwrap().is_empty());
    }

    /// Y si ya arranco, ademas de sacarlo de la cola pide la cancelacion, que
    /// es la señal que el scheduler levanta para matar el subproceso.
    #[test]
    fn cancelar_un_item_corriendo_pide_cortar_el_run() {
        let s = store();
        s.enqueue("d", "p", 0).unwrap();
        let r = s.create_run("d", "p").unwrap();
        {
            let c = s.conn.lock().unwrap();
            c.execute("UPDATE queue SET state = 'running', taken_at = 1", [])
                .unwrap();
        }
        let id = s.queue_items().unwrap()[0].id;

        let run = s.cancelar_item_cola(id).unwrap();
        assert_eq!(run, Some(r), "devuelve el run que mando a cortar");
        assert!(s.cancel_requested(r), "y deja pedida la cancelacion");
        assert!(s.queue_items().unwrap().is_empty());
    }

    /// Un id que no existe da error, no un ok silencioso: el boton tiene que
    /// poder decir que no pudo.
    #[test]
    fn cancelar_un_item_que_no_existe_avisa() {
        let s = store();
        assert!(s.cancelar_item_cola(9999).is_err());
    }
}
