//! # zeruel
//!
//! Motor de orquestacion de pipelines ETL. Un DAG se declara en Rust, compila a
//! un binario propio y corre con durabilidad por checkpoint, reintentos con
//! backoff, cancelacion y metricas de RAM/CPU por tarea.
//!
//! ## El modelo: un binario por pipeline
//!
//! Cada pipeline es **su propio ejecutable**, no un plugin ni una entrada en un
//! registro compilado. Eso es lo que hace que los pipelines sean independientes:
//! sumar uno es compilar un crate y copiar un archivo, y un panic o un OOM se
//! lleva puesto ese pipeline y nada mas.
//!
//! ```no_run
//! use zeruel::prelude::*;
//!
//! struct Bajar;
//! impl Task for Bajar {
//!     fn id(&self) -> TaskId { TaskId("bajar".into()) }
//!     fn run(&self, _ctx: &TaskContext) -> TaskResult {
//!         Ok(TaskOutput { rows: 100, message: "listo".into() })
//!     }
//! }
//!
//! fn main() -> zeruel::Result<()> {
//!     let dag = zeruel::dag!("ventas", "bajar" => []);
//!     zeruel::pipeline(dag)
//!         .register(Bajar)
//!         .start()
//! }
//! ```
//!
//! ## El contrato del binario
//!
//! [`PipelineBuilder::start`] le da al ejecutable dos modos, que son los que usa
//! `zeruel serve` para orquestarlo sin saber nada de su codigo:
//!
//! | Invocacion | Que hace |
//! |---|---|
//! | `mi-etl --describe` | Imprime el grafo y el schedule como JSON. |
//! | `mi-etl run --param X --state DIR` | Ejecuta el run y escribe los checkpoints. |
//!
//! El JSON de `--describe` lleva `zeruel_contract` como version: el scheduler
//! rechaza lo que no sabe leer en vez de adivinar.
//!
//! ## Features
//!
//! Por default no se compila ni polars ni wasmtime — juntos son el 47% del arbol
//! de dependencias. Se activan con `sql`, `wasm` y `polars` segun haga falta.

#![warn(missing_docs)]

mod bundle;
pub use bundle::{bundle, Bundle};

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

pub use zeruel_sdk::{
    dag, Dag, DagError, SqlSpec, Task, TaskContext, TaskDef, TaskId, TaskOutput, TaskState,
    WasmSpec,
};

pub use zeruel_core::{Engine, Run, RunReport, RunState, Store};

#[cfg(feature = "polars")]
#[cfg_attr(docsrs, doc(cfg(feature = "polars")))]
pub use zeruel_data::{Chequeo, ChequeoHistorico, DataEnv, ExportCsvTask, JoinTask, PolarsTask};

#[cfg(feature = "shell")]
#[cfg_attr(docsrs, doc(cfg(feature = "shell")))]
pub use zeruel_tasks::{ShellTask, PREFIJO_FILAS};

/// Lo que hace falta para escribir un pipeline. `use zeruel::prelude::*;`
pub mod prelude {
    pub use crate::{pipeline, Dag, Task, TaskContext, TaskId, TaskOutput, TaskResult};
    #[cfg(feature = "polars")]
    pub use zeruel_data::{
        Chequeo, ChequeoHistorico, DataEnv, ExportCsvTask, JoinTask, PolarsTask,
    };
    pub use zeruel_sdk::dag;
    #[cfg(feature = "shell")]
    pub use zeruel_tasks::ShellTask;
}

/// El resultado de una tarea.
///
/// El error es un `anyhow::Error`, que absorbe cualquier `std::error::Error`
/// con `?`. Para *construir* uno, esta [`falla`] o los macros re-exportados en
/// [`anyhow`] — no hace falta agregar la dependencia a tu crate.
pub type TaskResult = anyhow::Result<TaskOutput>;

/// Re-exportado para que un pipeline no tenga que declarar la dependencia solo
/// para escribir `bail!`.
///
/// ```no_run
/// # use zeruel::prelude::*;
/// # fn ejemplo(hay_datos: bool) -> TaskResult {
/// if !hay_datos {
///     zeruel::anyhow::bail!("la fuente no devolvio filas");
/// }
/// # Ok(TaskOutput::default())
/// # }
/// ```
pub use anyhow;

/// Corta la tarea con un mensaje.
///
/// Es lo mismo que `anyhow::anyhow!`, con un nombre que se lee en el contexto:
/// `return Err(zeruel::falla("..."))`.
pub fn falla(msg: impl Into<String>) -> anyhow::Error {
    anyhow::Error::msg(msg.into())
}

/// El resultado de una operacion de zeruel.
pub type Result<T> = std::result::Result<T, Error>;

/// Lo que puede salir mal al armar o correr un pipeline.
///
/// Es un enum y no un `anyhow::Error` a proposito: quien use zeruel como libreria
/// tiene que poder distinguir "el DAG esta mal declarado" de "fallo una tarea"
/// sin parsear el texto del mensaje.
#[derive(thiserror::Error)]
pub enum Error {
    /// El grafo no es valido: dependencia inexistente, id repetido o ciclo.
    #[error("el DAG '{dag}' no es valido: {source}")]
    Dag {
        /// Nombre del DAG.
        dag: String,
        /// La causa concreta.
        #[source]
        source: DagError,
    },

    /// Una tarea del grafo no tiene implementacion registrada.
    ///
    /// Se detecta al construir el pipeline, no a mitad del run.
    #[error("la tarea '{task}' del DAG '{dag}' no tiene implementacion registrada (falta un .register())")]
    TareaSinImplementar {
        /// Nombre del DAG.
        dag: String,
        /// Id de la tarea que falta.
        task: String,
    },

    /// El pipeline se invoco mal desde la linea de comandos.
    #[error("{0}")]
    Uso(String),

    /// Fallo el store, el engine o una tarea.
    #[error(transparent)]
    Ejecucion(#[from] anyhow::Error),
}

/// `Debug` a mano para que `fn main() -> zeruel::Result<()>` imprima algo legible.
///
/// Rust usa el `Debug` del error para reportar un `main` que devuelve `Err`, asi
/// que derivarlo mostraria `Uso("falta --param")` — con el nombre de la variante
/// y las comillas — en la cara de alguien que solo se equivoco de flag. Esto
/// imprime el mensaje y su cadena de causas, que es lo que sirve.
impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")?;
        let mut causa = std::error::Error::source(self);
        while let Some(c) = causa {
            write!(f, "\n  causado por: {c}")?;
            causa = std::error::Error::source(c);
        }
        Ok(())
    }
}

/// Empieza a construir un pipeline a partir de su grafo.
///
/// Ver [`PipelineBuilder`].
pub fn pipeline(dag: Dag) -> PipelineBuilder {
    PipelineBuilder {
        dag,
        tasks: HashMap::new(),
        fuentes: Vec::new(),
    }
}

/// Construye un pipeline: asocia cada nodo del grafo con su implementacion.
///
/// La validacion ocurre al construir ([`PipelineBuilder::build`] y
/// [`PipelineBuilder::start`]), no a mitad del run: si falta una tarea o el
/// grafo tiene un ciclo, se sabe antes de tocar un solo dato.
pub struct PipelineBuilder {
    dag: Dag,
    tasks: HashMap<TaskId, Arc<dyn Task>>,
    /// (ruta, contenido) de los archivos capturados con [`fuente!`].
    fuentes: Vec<(&'static str, &'static str)>,
}

/// Captura el codigo fuente del pipeline **al compilar**, para poder verlo
/// despues desde la interfaz.
///
/// ```ignore
/// pipeline(dag)
///     .fuente(zeruel::fuente!())          // src/main.rs
///     .fuente(zeruel::fuente!("src/tareas.rs"))
/// ```
///
/// ## Por que se empotra y no se lee del repositorio
///
/// Un pipeline se despliega copiando su binario, y ese binario puede tener
/// semanas mientras el repositorio siguio cambiando. Leer el codigo del
/// repositorio para mostrarlo al lado de una corrida vieja mostraria el de HOY:
/// no el que corrio. Eso es peor que no mostrar nada, porque **parece
/// confiable** — alguien depurando un fallo de hace tres dias miraria lineas
/// que no existian cuando fallo.
///
/// Empotrado con `include_str!` no puede mentir: son exactamente los bytes que
/// el compilador leyo para producir ese ejecutable.
///
/// El costo es el tamaño del archivo (unos pocos KB) y una linea por archivo
/// que se quiera ver. Es opcional: sin `.fuente()` el pipeline anda igual y la
/// interfaz dice que no hay codigo empotrado.
#[macro_export]
macro_rules! fuente {
    () => {
        $crate::fuente!("src/main.rs")
    };
    ($ruta:expr) => {
        (
            $ruta,
            include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $ruta)),
        )
    };
}

impl PipelineBuilder {
    /// Suma un archivo de codigo, capturado al compilar con [`fuente!`].
    pub fn fuente(mut self, f: (&'static str, &'static str)) -> Self {
        self.fuentes.push(f);
        self
    }

    /// Registra la implementacion de una tarea.
    ///
    /// El id sale del propio `Task`, asi que no hay forma de asociarlo a la
    /// clave equivocada.
    pub fn register(mut self, task: impl Task + 'static) -> Self {
        let t: Arc<dyn Task> = Arc::new(task);
        self.tasks.insert(t.id(), t);
        self
    }

    /// Registra una tarea ya envuelta en `Arc` (para compartirla entre DAGs).
    pub fn register_arc(mut self, task: Arc<dyn Task>) -> Self {
        self.tasks.insert(task.id(), task);
        self
    }

    /// Valida el grafo y que todas las tareas tengan implementacion.
    ///
    /// Las tareas con componente `wasm` o `sql` declarado en el DAG no hace
    /// falta registrarlas: las resuelve el engine.
    pub fn build(self) -> Result<Pipeline> {
        self.dag.validate().map_err(|source| Error::Dag {
            dag: self.dag.name.clone(),
            source,
        })?;

        for def in &self.dag.tasks {
            let la_resuelve_el_engine = def.wasm.is_some() || def.sql.is_some();
            if !la_resuelve_el_engine && !self.tasks.contains_key(&def.id) {
                return Err(Error::TareaSinImplementar {
                    dag: self.dag.name.clone(),
                    task: def.id.0.clone(),
                });
            }
        }

        Ok(Pipeline {
            dag: self.dag,
            tasks: self.tasks,
            fuentes: self.fuentes,
        })
    }

    /// Construye el pipeline y le da al binario su interfaz de linea de comandos.
    ///
    /// Es lo que convierte un `main()` en un pipeline orquestable: implementa el
    /// contrato (`--describe` y `run`) que usa `zeruel serve` para lanzarlo como
    /// subproceso. Lee los argumentos del proceso.
    pub fn start(self) -> Result<()> {
        self.build()?.main()
    }
}

/// Un pipeline validado y listo para correr.
pub struct Pipeline {
    dag: Dag,
    tasks: HashMap<TaskId, Arc<dyn Task>>,
    fuentes: Vec<(&'static str, &'static str)>,
}

/// A mano y no derivado: `Arc<dyn Task>` no es `Debug`, y ademas lo util al
/// depurar son los ids, no el contenido de cada tarea.
impl std::fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ids: Vec<&str> = self.tasks.keys().map(|t| t.0.as_str()).collect();
        ids.sort_unstable();
        f.debug_struct("Pipeline")
            .field("dag", &self.dag.name)
            .field("tareas", &ids)
            .finish()
    }
}

impl Pipeline {
    /// El grafo.
    pub fn dag(&self) -> &Dag {
        &self.dag
    }

    /// Corre el pipeline una vez contra un store ya abierto.
    ///
    /// Para uso embebido (dentro de otro programa). Un binario de pipeline usa
    /// [`Pipeline::main`], que ademas le da la CLI del contrato.
    pub fn run(&self, store: Arc<Store>, param: &str, workers: usize) -> Result<RunReport> {
        self.run_con(store, param, workers, None, 0)
    }

    /// Corre el pipeline con todas las opciones.
    ///
    /// `techo_mb` en 0 es sin techo. `run_id` adopta un run que ya creo el
    /// scheduler.
    pub fn run_con(
        &self,
        store: Arc<Store>,
        param: &str,
        workers: usize,
        run_id: Option<uuid::Uuid>,
        techo_mb: u64,
    ) -> Result<RunReport> {
        let engine = Engine::new(store).con_techo_mb(techo_mb);
        Ok(engine.run_con_id(&self.dag, &self.tasks, param, workers, run_id)?)
    }

    /// Igual que [`Pipeline::run`], pero adoptando un run ya creado por el
    /// scheduler (el `--run-id` del contrato).
    pub fn run_con_id(
        &self,
        store: Arc<Store>,
        param: &str,
        workers: usize,
        run_id: uuid::Uuid,
    ) -> Result<RunReport> {
        self.run_con(store, param, workers, Some(run_id), 0)
    }

    /// El JSON de `--describe`: lo que el scheduler necesita saber del pipeline
    /// sin ejecutarlo.
    ///
    /// Un pipeline suelto se describe como un bundle de uno, para que el
    /// scheduler lea una sola forma.
    pub fn describe(&self) -> serde_json::Value {
        serde_json::json!({
            "zeruel_contract": CONTRATO,
            "pipelines": [self.describe_uno()],
        })
    }

    /// El codigo empotrado, como JSON.
    ///
    /// Vacio si el autor no llamo a `.fuente(zeruel::fuente!())`: es opcional,
    /// y la interfaz tiene que poder decir "este pipeline no trae codigo" en
    /// vez de mostrar una pantalla rota.
    pub fn fuente_json(&self) -> serde_json::Value {
        serde_json::json!({
            "zeruel_contract": CONTRATO,
            "name": self.dag.name,
            "archivos": self
                .fuentes
                .iter()
                .map(|(ruta, texto)| serde_json::json!({
                    "ruta": ruta,
                    "texto": texto,
                    "lineas": texto.lines().count(),
                }))
                .collect::<Vec<_>>(),
        })
    }

    /// La entrada de este pipeline dentro de un `--describe`.
    pub(crate) fn describe_uno(&self) -> serde_json::Value {
        let tasks: Vec<_> = self
            .dag
            .tasks
            .iter()
            .map(|t| {
                serde_json::json!({
                    "id": t.id.0,
                    "deps": t.deps.iter().map(|d| &d.0).collect::<Vec<_>>(),
                    "kind": if t.wasm.is_some() { "wasm" }
                            else if t.sql.is_some() { "sql" }
                            else { "rust" },
                })
            })
            .collect();
        serde_json::json!({
            "name": self.dag.name,
            "schedule": self.dag.schedule,
            "max_retries": self.dag.max_retries,
            "tasks": tasks,
        })
    }

    /// Implementa el contrato del binario de pipeline leyendo `std::env::args`.
    pub fn main(&self) -> Result<()> {
        self.main_con(std::env::args().skip(1).collect())
    }

    /// Igual que [`Pipeline::main`] pero con los argumentos explicitos (testeable).
    pub fn main_con(&self, args: Vec<String>) -> Result<()> {
        match args.first().map(String::as_str) {
            Some("--describe") => {
                println!("{}", self.describe());
                Ok(())
            }
            // El codigo que se empotro al compilar. Se imprime como JSON para
            // que el scheduler lo lea igual que `--describe`.
            Some("--fuente") => {
                println!("{}", self.fuente_json());
                Ok(())
            }
            Some("run") => {
                let opts = RunArgs::parse(&args[1..])?;
                init_logs();
                let store = Arc::new(
                    Store::open(&opts.state)
                        .map_err(|e| Error::Ejecucion(e.context("abrir el state")))?,
                );
                let rep = self.run_con(
                    store,
                    &opts.param,
                    opts.workers,
                    opts.run_id,
                    opts.ram_max_mb,
                )?;
                println!(
                    "{}",
                    serde_json::json!({
                        "run_id": rep.run_id.to_string(),
                        "ok": rep.success,
                        "cancelado": rep.cancelled,
                        "por_techo_de_memoria": rep.paso_el_techo,
                        "filas": rep.total_rows,
                        "reutilizadas": rep.skipped,
                    })
                );
                if rep.success {
                    Ok(())
                } else if rep.cancelled {
                    std::process::exit(EXIT_CANCELADO);
                } else {
                    std::process::exit(EXIT_FALLO);
                }
            }
            Some("--help") | Some("-h") | None => {
                println!("{}", self.ayuda());
                Ok(())
            }
            Some("--version") | Some("-V") => {
                println!("{} (zeruel contrato {})", self.dag.name, CONTRATO);
                Ok(())
            }
            Some(otro) => Err(Error::Uso(format!(
                "no entiendo '{otro}'.\n\n{}",
                self.ayuda()
            ))),
        }
    }

    fn ayuda(&self) -> String {
        format!(
            "{nombre} — pipeline de zeruel\n\
             \n\
             USO:\n  \
               <bin> run --param <VALOR> --state <DIR> [--workers N] [--run-id UUID]\n  \
               <bin> --describe        el grafo y el schedule, como JSON\n  \
               <bin> --version\n\
             \n\
             Tareas: {tareas}\n\
             Schedule: {sched}\n\
             \n\
             Si esta en el directorio de pipelines, `zeruel serve` lo lanza solo.",
            nombre = self.dag.name,
            tareas = self
                .dag
                .tasks
                .iter()
                .map(|t| t.id.0.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            sched = self
                .dag
                .schedule
                .as_deref()
                .unwrap_or("(solo bajo demanda)"),
        )
    }
}

/// Interpreta un techo de memoria escrito como lo escribe una persona.
///
/// Acepta `2G`, `2GB`, `512M`, `512MB` o un numero suelto en MB. Devuelve MB.
pub fn parsear_memoria(s: &str) -> Result<u64> {
    let t = s.trim().to_uppercase();
    let (num, mult) = if let Some(n) = t.strip_suffix("GB").or_else(|| t.strip_suffix("G")) {
        (n, 1024u64)
    } else if let Some(n) = t.strip_suffix("MB").or_else(|| t.strip_suffix("M")) {
        (n, 1)
    } else {
        (t.as_str(), 1)
    };
    let v: f64 = num.trim().parse().map_err(|_| {
        Error::Uso(format!(
            "no entiendo el techo de memoria {s:?}. Usa 2G, 512M o un numero en MB"
        ))
    })?;
    if v < 0.0 {
        return Err(Error::Uso(
            "el techo de memoria no puede ser negativo".into(),
        ));
    }
    Ok((v * mult as f64) as u64)
}

/// Version del contrato entre un binario de pipeline y `zeruel serve`.
///
/// Se sube cuando cambia la forma del JSON de `--describe` o los argumentos de
/// `run`. El scheduler rechaza lo que no sabe leer en vez de adivinar.
///
/// - **1**: un pipeline por binario. `--describe` devolvia el DAG suelto.
/// - **2**: varios pipelines por binario. `--describe` devuelve
///   `pipelines: [...]` y `run` acepta `--dag <nombre>`. El scheduler sigue
///   leyendo binarios de contrato 1 sin cambios.
pub const CONTRATO: u32 = 2;

/// Codigo de salida cuando el run termino cancelado.
pub const EXIT_CANCELADO: i32 = 3;
/// Codigo de salida cuando el run fallo.
pub const EXIT_FALLO: i32 = 1;

/// Argumentos de `run`.
#[derive(Debug)]
pub(crate) struct RunArgs {
    param: String,
    state: PathBuf,
    workers: usize,
    run_id: Option<uuid::Uuid>,
    /// Techo de RAM en MB (0 = sin techo).
    ram_max_mb: u64,
}

impl RunArgs {
    /// Parseo a mano, a proposito: `clap` son 4 crates mas en el arbol de
    /// dependencias de **cada** binario de pipeline, para una CLI de 4 flags.
    pub(crate) fn parse(args: &[String]) -> Result<Self> {
        let mut param = None;
        let mut state = None;
        let mut workers = 4usize;
        let mut run_id = None;
        let mut ram_max_mb = 0u64;

        let mut i = 0;
        while i < args.len() {
            let valor = |i: usize| -> Result<String> {
                args.get(i + 1)
                    .cloned()
                    .ok_or_else(|| Error::Uso(format!("a {} le falta el valor", args[i])))
            };
            match args[i].as_str() {
                "--param" | "-p" => {
                    param = Some(valor(i)?);
                    i += 2;
                }
                "--state" | "-s" => {
                    state = Some(PathBuf::from(valor(i)?));
                    i += 2;
                }
                "--workers" | "-w" => {
                    workers = valor(i)?
                        .parse()
                        .map_err(|_| Error::Uso("--workers tiene que ser un numero".into()))?;
                    i += 2;
                }
                "--ram-max" => {
                    ram_max_mb = parsear_memoria(&valor(i)?)?;
                    i += 2;
                }
                "--run-id" => {
                    run_id = Some(
                        valor(i)?
                            .parse()
                            .map_err(|_| Error::Uso("--run-id tiene que ser un UUID".into()))?,
                    );
                    i += 2;
                }
                otro => return Err(Error::Uso(format!("flag desconocido: {otro}"))),
            }
        }

        Ok(Self {
            param: param.ok_or_else(|| Error::Uso("falta --param".into()))?,
            state: state.ok_or_else(|| Error::Uso("falta --state".into()))?,
            workers: workers.max(1),
            run_id,
            ram_max_mb,
        })
    }
}

/// Logs a stdout. El binario de pipeline no rota archivos: cuando lo lanza
/// `zeruel serve`, el scheduler captura su salida y la manda al log del run.
pub(crate) fn init_logs() {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Falsa(&'static str);
    impl Task for Falsa {
        fn id(&self) -> TaskId {
            TaskId(self.0.into())
        }
        fn run(&self, _ctx: &TaskContext) -> TaskResult {
            Ok(TaskOutput {
                rows: 1,
                message: "ok".into(),
            })
        }
    }

    fn dag_dos() -> Dag {
        zeruel_sdk::dag!("prueba", "a" => [], "b" => ["a"])
    }

    #[test]
    fn falta_una_implementacion_se_detecta_al_construir() {
        let e = pipeline(dag_dos())
            .register(Falsa("a"))
            .build()
            .unwrap_err();
        match e {
            Error::TareaSinImplementar { task, dag } => {
                assert_eq!(task, "b");
                assert_eq!(dag, "prueba");
            }
            otro => panic!("esperaba TareaSinImplementar, vino {otro:?}"),
        }
        // el mensaje tiene que decir que hacer
        let e = pipeline(dag_dos())
            .register(Falsa("a"))
            .build()
            .unwrap_err();
        assert!(e.to_string().contains(".register()"), "{e}");
    }

    #[test]
    fn un_dag_invalido_se_detecta_al_construir() {
        let mut d = Dag::new("roto");
        d.tasks
            .push(TaskDef::new("a", vec![TaskId("no-existe".into())]));
        let e = pipeline(d).register(Falsa("a")).build().unwrap_err();
        assert!(matches!(e, Error::Dag { .. }), "vino {e:?}");
        assert!(e.to_string().contains("no-existe"), "{e}");
    }

    #[test]
    fn construye_cuando_estan_todas() {
        let p = pipeline(dag_dos())
            .register(Falsa("a"))
            .register(Falsa("b"))
            .build()
            .unwrap();
        assert_eq!(p.dag().tasks.len(), 2);
    }

    #[test]
    fn describe_trae_el_grafo_y_la_version_del_contrato() {
        let p = pipeline(dag_dos())
            .register(Falsa("a"))
            .register(Falsa("b"))
            .build()
            .unwrap();
        let d = p.describe();
        assert_eq!(d["zeruel_contract"], CONTRATO);

        // Un pipeline suelto se describe como un bundle de uno: el scheduler
        // lee una sola forma, venga de donde venga.
        let ps = d["pipelines"].as_array().unwrap();
        assert_eq!(ps.len(), 1);
        assert_eq!(ps[0]["name"], "prueba");
        let tasks = ps[0]["tasks"].as_array().unwrap();
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[1]["id"], "b");
        assert_eq!(tasks[1]["deps"][0], "a");
        assert_eq!(tasks[1]["kind"], "rust");
    }

    #[test]
    fn el_pipeline_corre_de_punta_a_punta() {
        let p = pipeline(dag_dos())
            .register(Falsa("a"))
            .register(Falsa("b"))
            .build()
            .unwrap();
        let store = Arc::new(Store::open_in_memory().unwrap());
        let rep = p.run(store.clone(), "2026-08-19", 2).unwrap();
        assert!(rep.success);
        assert_eq!(rep.total_rows, 2);
        assert_eq!(
            store.run_by_id(rep.run_id).unwrap().unwrap().param,
            "2026-08-19"
        );
    }

    #[test]
    fn adopta_el_run_que_creo_el_scheduler() {
        // El scheduler crea la fila del run para poder cancelarla, y le pasa el
        // id al subproceso.
        let store = Arc::new(Store::open_in_memory().unwrap());
        let id = store.create_run("prueba", "P1").unwrap();
        let p = pipeline(dag_dos())
            .register(Falsa("a"))
            .register(Falsa("b"))
            .build()
            .unwrap();
        let rep = p.run_con_id(store.clone(), "P1", 1, id).unwrap();
        assert_eq!(rep.run_id, id, "usa el run del scheduler, no crea otro");
        assert_eq!(
            store.recent_runs(10, 0).unwrap().len(),
            1,
            "no duplica el run"
        );
    }

    // ---- el contrato de la CLI ----

    #[test]
    fn parsea_el_techo_como_lo_escribe_una_persona() {
        assert_eq!(parsear_memoria("2G").unwrap(), 2048);
        assert_eq!(parsear_memoria("2GB").unwrap(), 2048);
        assert_eq!(parsear_memoria("512M").unwrap(), 512);
        assert_eq!(parsear_memoria("512MB").unwrap(), 512);
        assert_eq!(parsear_memoria("1024").unwrap(), 1024, "sin sufijo son MB");
        assert_eq!(
            parsear_memoria(" 4g ").unwrap(),
            4096,
            "tolera espacios y minusculas"
        );
        assert_eq!(parsear_memoria("1.5G").unwrap(), 1536, "admite decimales");
        let e = parsear_memoria("mucha").unwrap_err().to_string();
        assert!(e.contains("2G"), "el error muestra el formato: {e}");
    }

    #[test]
    fn el_techo_llega_por_la_linea_de_comandos() {
        let a = RunArgs::parse(&[
            "--param".into(),
            "p".into(),
            "--state".into(),
            "s".into(),
            "--ram-max".into(),
            "2G".into(),
        ])
        .unwrap();
        assert_eq!(a.ram_max_mb, 2048);
        // sin el flag, sin techo
        let b =
            RunArgs::parse(&["--param".into(), "p".into(), "--state".into(), "s".into()]).unwrap();
        assert_eq!(b.ram_max_mb, 0);
    }

    #[test]
    fn un_techo_mal_escrito_se_rechaza_antes_de_correr() {
        let e = RunArgs::parse(&[
            "--param".into(),
            "p".into(),
            "--state".into(),
            "s".into(),
            "--ram-max".into(),
            "dos gigas".into(),
        ])
        .unwrap_err()
        .to_string();
        assert!(e.contains("no entiendo el techo"), "{e}");
    }

    #[test]
    fn describe_por_linea_de_comandos() {
        let p = pipeline(dag_dos())
            .register(Falsa("a"))
            .register(Falsa("b"))
            .build()
            .unwrap();
        p.main_con(vec!["--describe".into()]).unwrap();
    }

    #[test]
    fn run_sin_param_lo_dice_claro() {
        let p = pipeline(dag_dos())
            .register(Falsa("a"))
            .register(Falsa("b"))
            .build()
            .unwrap();
        let e = p
            .main_con(vec!["run".into(), "--state".into(), "x".into()])
            .unwrap_err();
        assert!(e.to_string().contains("--param"), "{e}");
    }

    #[test]
    fn flag_desconocido_no_se_ignora_en_silencio() {
        let p = pipeline(dag_dos())
            .register(Falsa("a"))
            .register(Falsa("b"))
            .build()
            .unwrap();
        let e = p
            .main_con(vec!["run".into(), "--parm".into(), "x".into()])
            .unwrap_err();
        assert!(e.to_string().contains("--parm"), "{e}");
    }

    #[test]
    fn subcomando_desconocido_muestra_la_ayuda() {
        let p = pipeline(dag_dos())
            .register(Falsa("a"))
            .register(Falsa("b"))
            .build()
            .unwrap();
        let e = p.main_con(vec!["correr".into()]).unwrap_err();
        let msg = e.to_string();
        assert!(msg.contains("correr"));
        assert!(
            msg.contains("--describe"),
            "la ayuda tiene que estar: {msg}"
        );
    }
}
