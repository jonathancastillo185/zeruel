//! zeruel-sdk: API para declarar DAGs y tareas de zeruel.
//!
//! Un DAG se arma con la macro [`dag!`] (lo comun) o con el builder
//! (`Dag::new` + `dag.tasks.push(TaskDef::..)`), que es lo que hace falta para
//! las tareas con componente wasm/sql.
//!
//! B.1 (Fase 5): una tarea del DAG puede ser un modulo `.wasm` corriendo en el
//! sandbox: se declara `TaskDef.wasm = Some(WasmSpec {..})` y el engine la
//! compila y ejecuta igual que cualquier otra tarea (deps + param + rows +
//! retries).
//!
//! B.2 (Fase 3): una tarea del DAG puede ser una query SQL sobre Parquet
//! (motor polars-sql): se declara `TaskDef.sql = Some(SqlSpec {..})`.
//! El engine la despacha igual que cualquier otra tarea.

use std::collections::HashMap;

/// Contexto que el core le pasa a cada tarea al ejecutarla.
pub struct TaskContext {
    /// Run al que pertenece (para logging y checkpoints).
    pub run_id: uuid::Uuid,
    /// Parametro de negocio (ej: la fecha del run).
    pub param: String,
    /// Senal de cancelacion (UI / API). `None` = no se puede cancelar.
    /// La tarea (o el engine entre tasks) la consulta; si se setea, el run
    /// se corta: el sandbox wasm la usa para abortar el codigo aereo en vivo.
    pub cancel: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
}

/// Lo que una tarea puede hacer cuando el core la ejecuta.
///
/// Ojo: la tarea NO declara sus dependencias. El grafo vive en el [`Dag`]
/// (`TaskDef.deps`) y es lo unico que mira el engine; una tarea es solo la
/// implementacion de un nodo. Hasta 2026-08-17 el trait tenia un `deps()` que
/// todas las implementaciones devolvian vacio y que el engine nunca llamaba:
/// era una trampa (quien lo completaba veia su orden ignorado en silencio).
pub trait Task: Send + Sync {
    fn id(&self) -> TaskId;
    fn run(&self, ctx: &TaskContext) -> anyhow::Result<TaskOutput>;
}

/// Salida estandar de una tarea: filas procesadas + mensaje para el log.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
pub struct TaskOutput {
    pub rows: u64,
    pub message: String,
}

/// Identificador estable de una tarea dentro de un DAG.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TaskId(pub String);

/// Estado de una tarea en un run (lo que checkpointea el core).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TaskState {
    Pending,
    Running,
    Success,
    Failed { retries_left: u32 },
}

/// Config de una tarea WASM dentro de un DAG (B.1 / Fase 5): el path al modulo
/// `.wasm` mas los limites del sandbox. El engine lo compila UNA vez por run
/// y lo despacha como una tarea mas del grafo.
#[derive(Clone, Debug)]
pub struct WasmSpec {
    /// Ruta al modulo `.wasm` (mismo ABI: exporta `buf_ptr` y `task_run`).
    pub path: std::path::PathBuf,
    /// Tope de instrucciones WASM (fuel). Default 100_000_000.
    pub fuel: u64,
    /// Tope de memoria lineal en MB (paginas de 64KB). Default 16.
    pub mem_mb: u32,
}

impl WasmSpec {
    /// Conviene: path + limites por defecto (fuel 100M, 16MB).
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            path: path.into(),
            fuel: 100_000_000,
            mem_mb: 16,
        }
    }
}

/// Config de una tarea SQL dentro de un DAG (B.2 / Fase 3): una query SQL
/// (motor polars-sql) sobre tablas registradas como Parquet.
///
/// El engine la ejecuta como tarea de primera: deps/param/rows/retries genericos.
#[derive(Clone, Debug)]
pub struct SqlSpec {
    /// Query SQL a ejecutar (ej: `SELECT * FROM ventas WHERE monto > 0`).
    /// Las tablas disponibles son las keys de `tables`.
    pub sql: String,
    /// Mapeo nombre_tabla → path al archivo Parquet.
    /// Ej: `{"ventas": "data/clean/ventas.parquet"}`.
    /// Los paths son absolutos (los resuelve el caller con el DataEnv).
    pub tables: HashMap<String, std::path::PathBuf>,
    /// Ruta de salida del resultado (Parquet). Se sobreescribe en cada run.
    pub output: std::path::PathBuf,
}

/// Un nodo del grafo: nombre + dependencias. El componente de ejecucion:
///   - `wasm: Some`  → modulo `.wasm` en sandbox (el engine lo compila).
///   - `sql: Some`   → query SQL sobre Parquet (el engine la ejecuta).
///   - ambos `None`  → la implementacion la provee el hashmap del caller.
#[derive(Clone, Debug)]
pub struct TaskDef {
    pub id: TaskId,
    pub deps: Vec<TaskId>,
    pub wasm: Option<WasmSpec>,
    pub sql: Option<SqlSpec>,
}

impl TaskDef {
    /// Tarea nativa (Rust/SDK): la implementacion la registra el caller.
    /// `wasm: None`, `sql: None`.
    pub fn new(id: impl Into<String>, deps: Vec<TaskId>) -> Self {
        Self {
            id: TaskId(id.into()),
            deps,
            wasm: None,
            sql: None,
        }
    }

    /// Tarea WASM: el engine compila `spec.path` y la ejecuta en el sandbox.
    pub fn wasm(id: impl Into<String>, deps: Vec<TaskId>, spec: WasmSpec) -> Self {
        Self {
            id: TaskId(id.into()),
            deps,
            wasm: Some(spec),
            sql: None,
        }
    }

    /// Tarea SQL: el engine ejecuta `spec.sql` sobre las tablas registradas
    /// (polars-sql) y escribe el resultado Parquet en `spec.output`.
    pub fn sql(id: impl Into<String>, deps: Vec<TaskId>, spec: SqlSpec) -> Self {
        Self {
            id: TaskId(id.into()),
            deps,
            wasm: None,
            sql: Some(spec),
        }
    }
}

/// Un DAG completo, listo para ser ejecutado por zeruel-core.
#[derive(Clone, Debug)]
pub struct Dag {
    pub name: String,
    pub tasks: Vec<TaskDef>,
    /// Schedule cron (opcional; None = solo se corre bajo demanda).
    pub schedule: Option<String>,
    /// Retries por defecto si una tarea falla.
    pub max_retries: u32,
}

impl Dag {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            tasks: Vec::new(),
            schedule: None,
            max_retries: 3,
        }
    }

    /// Valida el grafo antes de recorrerlo: ids unicos y deps existentes.
    ///
    /// Va aparte de Kahn a proposito: si se cuentan las deps declaradas (en vez
    /// de las que existen), una dep con typo deja un nodo con grado que nunca
    /// baja y el algoritmo lo reporta como "ciclo", que manda a buscar un
    /// problema que no existe. Con la macro `dag!` (ids como strings sueltos,
    /// sin chequeo del compilador) el typo es el error mas probable de todos.
    pub fn validate(&self) -> Result<(), DagError> {
        let mut vistos: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for t in &self.tasks {
            if !vistos.insert(t.id.0.as_str()) {
                return Err(DagError::DuplicateTask {
                    task: t.id.0.clone(),
                });
            }
        }
        for t in &self.tasks {
            for d in &t.deps {
                if !vistos.contains(d.0.as_str()) {
                    return Err(DagError::UnknownDep {
                        task: t.id.0.clone(),
                        dep: d.0.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Orden topologico del grafo (Kahn, raiz primero).
    ///
    /// Errores: [`DagError::DuplicateTask`] si hay ids repetidos,
    /// [`DagError::UnknownDep`] si una dep no existe, [`DagError::Cycle`] si
    /// hay un ciclo de verdad.
    pub fn topological_order(&self) -> Result<Vec<&TaskDef>, DagError> {
        self.validate()?;
        // Grado de entrada = deps que existen en el grafo (ya validadas).
        let mut deg: HashMap<&TaskId, usize> =
            self.tasks.iter().map(|t| (&t.id, t.deps.len())).collect();
        let mut ready: Vec<&TaskDef> = self.tasks.iter().filter(|t| t.deps.is_empty()).collect();
        let mut out = Vec::with_capacity(self.tasks.len());
        while let Some(t) = ready.pop() {
            out.push(t);
            for u in self.tasks.iter() {
                if u.deps.contains(&t.id) {
                    let d = deg.get_mut(&u.id).expect("id validado");
                    *d -= 1;
                    if *d == 0 {
                        ready.push(u);
                    }
                }
            }
        }
        if out.len() != self.tasks.len() {
            return Err(DagError::Cycle);
        }
        Ok(out)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DagError {
    #[error("el DAG tiene un ciclo de dependencias")]
    Cycle,
    #[error("la tarea '{task}' depende de '{dep}', que no existe en el DAG")]
    UnknownDep { task: String, dep: String },
    #[error("la tarea '{task}' esta declarada mas de una vez en el DAG")]
    DuplicateTask { task: String },
}

/// Macro declarativa para armar un `Dag` en una sola expresion.
///
/// Sintaxis:
/// ```ignore
/// let dag = zeruel_sdk::dag!("nombre_dag",
///     "validar"        => [],
///     "limpiar"        => ["validar"],
///     "export_csv"     => ["rollups"],
/// );
/// ```
///
/// Cada linea es `"<id>" => ["<dep1>", "<dep2>", ...]`. Genera
/// `Dag::new(name).task(...).task(...)` con ids como `TaskId(String::from)`.
/// Para tareas con componente (wasm/sql) hay que agregarlas despues con
/// `dag.tasks.push(TaskDef::wasm(...))` / `TaskDef::sql(...)`: la macro
/// cubre el 95% del caso (tareas nativas registradas por el caller).
#[macro_export]
macro_rules! dag {
    ($nombre:expr) => {
        $crate::Dag::new($nombre)
    };
    ($nombre:expr, $( $id:expr => $deps:expr ),+ $(,)?) => {
        {
            let mut d = $crate::Dag::new($nombre);
            $(
                d.tasks.push($crate::TaskDef::new($id, $deps.into_iter().map(|s: &str| $crate::TaskId(String::from(s))).collect()));
            )+
            d
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(s: &str) -> TaskId {
        TaskId(s.into())
    }

    #[test]
    fn orden_topologico_lineal() {
        let mut dag = Dag::new("t");
        dag.tasks.push(TaskDef::new("a", vec![]));
        dag.tasks.push(TaskDef::new("b", vec![id("a")]));
        dag.tasks.push(TaskDef::new("c", vec![id("a")]));
        let order = dag.topological_order().unwrap();
        let names: Vec<_> = order.iter().map(|t| t.id.0.as_str()).collect();
        assert_eq!(names[0], "a");
        assert!(names.contains(&"b") && names.contains(&"c"));
    }

    #[test]
    fn detecta_ciclo() {
        let mut dag = Dag::new("t");
        dag.tasks.push(TaskDef::new("a", vec![id("b")]));
        dag.tasks.push(TaskDef::new("b", vec![id("a")]));
        assert!(matches!(dag.topological_order(), Err(DagError::Cycle)));
    }

    #[test]
    fn dep_inexistente_no_es_ciclo() {
        // Regresion N-14: antes esto devolvia Cycle y mandaba a buscar un ciclo
        // inexistente. Un typo en una dep tiene que decir cual es.
        let mut dag = Dag::new("t");
        dag.tasks.push(TaskDef::new("a", vec![]));
        dag.tasks.push(TaskDef::new("b", vec![id("validr")])); // typo de "validar"
        match dag.topological_order() {
            Err(DagError::UnknownDep { task, dep }) => {
                assert_eq!(task, "b");
                assert_eq!(dep, "validr");
            }
            otro => panic!("esperaba UnknownDep, vino {otro:?}"),
        }
    }

    #[test]
    fn detecta_task_duplicada() {
        let mut dag = Dag::new("t");
        dag.tasks.push(TaskDef::new("a", vec![]));
        dag.tasks.push(TaskDef::new("a", vec![]));
        assert!(matches!(
            dag.topological_order(),
            Err(DagError::DuplicateTask { .. })
        ));
    }

    #[test]
    fn orden_topologico_respeta_deps_en_diamante() {
        // a -> {b, c} -> d: d tiene que salir despues de b y c.
        let d = dag!("diamante",
            "a" => [],
            "b" => ["a"],
            "c" => ["a"],
            "d" => ["b", "c"],
        );
        let order = d.topological_order().unwrap();
        let pos = |n: &str| order.iter().position(|t| t.id.0 == n).unwrap();
        assert!(pos("a") < pos("b") && pos("a") < pos("c"));
        assert!(pos("b") < pos("d") && pos("c") < pos("d"));
    }

    #[test]
    fn wasm_spec_por_defecto() {
        let s = WasmSpec::new("x.wasm");
        assert_eq!(s.fuel, 100_000_000);
        assert_eq!(s.mem_mb, 16);
        let d = TaskDef::wasm("w", vec![], s);
        assert!(d.wasm.is_some());
        assert!(d.sql.is_none());
        assert!(TaskDef::new("w", vec![]).wasm.is_none());
    }

    #[test]
    fn sql_spec_en_taskdef() {
        let spec = SqlSpec {
            sql: "SELECT * FROM t".into(),
            tables: HashMap::new(),
            output: std::path::PathBuf::from("out.parquet"),
        };
        let d = TaskDef::sql("q", vec![], spec);
        assert!(d.sql.is_some());
        assert!(d.wasm.is_none());
    }

    #[test]
    fn macro_dag_lineal() {
        let d = dag!("t",
            "a" => [],
            "b" => ["a"],
            "c" => ["a"],
        );
        assert_eq!(d.name, "t");
        assert_eq!(d.tasks.len(), 3);
        let order = d.topological_order().unwrap();
        assert_eq!(order[0].id.0, "a");
    }

    #[test]
    fn macro_dag_vacia() {
        let d = dag!("solo_nombre");
        assert_eq!(d.name, "solo_nombre");
        assert!(d.tasks.is_empty());
    }
}

/// Pico de memoria aportado por procesos hijos que ya terminaron.
///
/// # Por que hace falta un buzon global
///
/// Medir la memoria de un pipeline se hace muestreando el arbol de procesos, y
/// descubrir ese arbol obliga a enumerar los del sistema: 39 ms con los 518 de
/// una maquina real. O sea que el arbol no se puede redescubrir mas seguido que
/// cada medio segundo, y **un hijo que nace y muere en el medio no se ve nunca**.
/// No es raro: python reserva 300 MB y sale en 0,12 s.
///
/// Quien SI lo ve es el que lo lanzo, porque tiene su handle y el sistema
/// operativo le sabe decir el pico exacto aunque el proceso ya no exista. El
/// problema es que el que lanza (`zeruel-tasks`) y el que mide
/// (`zeruel-core`) no se conocen entre si: los dos dependen de este crate y de
/// ninguno de los dos depende el otro. Este buzon es el punto de encuentro.
///
/// El valor es **RSS**, no memoria comprometida. La distincion importa: en un
/// Python con pandas el commit es 5 veces el RSS (401 MB contra 77 medidos), y
/// usar el numero equivocado para un techo de memoria hace que se corten
/// corridas que nunca se pasaron de nada.
pub mod pico_hijo {
    use std::sync::atomic::{AtomicU64, Ordering};

    static PICO_MB: AtomicU64 = AtomicU64::new(0);

    /// Lo pone en cero. El medidor lo llama al empezar cada tarea: si no, una
    /// tarea heredaria el pico del hijo de la anterior.
    pub fn reiniciar() {
        PICO_MB.store(0, Ordering::Relaxed);
    }

    /// Deja el pico de un hijo que ya termino. Se queda con el mayor.
    pub fn aportar(mb: u64) {
        PICO_MB.fetch_max(mb, Ordering::Relaxed);
    }

    /// El mayor aporte desde el ultimo `reiniciar`.
    pub fn leer() -> u64 {
        PICO_MB.load(Ordering::Relaxed)
    }
}
