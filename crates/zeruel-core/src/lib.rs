//! Nucleo de zeruel: el metadata store (SQLite-WAL) y el engine que ejecuta un DAG.
//!
//! # Features
//!
//! Por default este crate compila **solo** el engine, el store y las metricas.
//! Los dos motores opcionales estan detras de features porque son el grueso del
//! arbol de dependencias, y no todo pipeline los necesita:
//!
//! | Feature | Que agrega | Costo |
//! |---|---|---|
//! | `sql` | [`SqlTask`]: queries polars-sql sobre Parquet | polars, ~155 crates |
//! | `wasm` | [`WasmTask`]: sandbox WASM con limites de fuel y memoria | wasmtime, ~31 crates |
//!
//! Un DAG puede declarar tareas `sql` o `wasm` aunque el binario se haya
//! compilado sin esos features; en ese caso el engine falla al arrancar el run
//! con un mensaje que dice exactamente eso, en vez de ignorarlas en silencio.

pub mod corridas;
pub mod engine;
pub mod limite;
pub mod metricas;
pub mod metrics;
pub mod pico_os;
pub mod store;

#[cfg(feature = "sql")]
#[cfg_attr(docsrs, doc(cfg(feature = "sql")))]
pub mod sql;

#[cfg(feature = "wasm")]
#[cfg_attr(docsrs, doc(cfg(feature = "wasm")))]
pub mod wasm;

pub use engine::{Engine, RunReport};
pub use store::{
    EnqueueResult, GcReport, Hygiene, PickResult, QueueItem, Run, RunState, Store, TaskRun,
};

#[cfg(feature = "sql")]
pub use sql::SqlTask;

#[cfg(feature = "wasm")]
pub use wasm::WasmTask;
