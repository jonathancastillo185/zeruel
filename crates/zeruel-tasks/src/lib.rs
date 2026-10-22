//! Tareas de proposito general para zeruel.
//!
//! Son los "operadores" que no dependen de polars: sirven para orquestar cosas
//! que ya existen y para traer datos de afuera.
//!
//! | Tarea | Feature | Para que |
//! |---|---|---|
//! | [`ShellTask`] | `shell` (por defecto) | Correr un comando: un script de Python, un `.jar`, un `curl` |
//! | [`HttpTask`] | `http` | Bajar de una API o una URL a un archivo, en streaming |
//! | [`PostgresSource`] / [`PostgresSink`] | `postgres` | Leer y cargar Postgres con `COPY`, sin pasar la tabla por memoria |
//! | [`SnowflakeStatement`] | `snowflake` | Ejecutar sentencias (el `COPY INTO` que carga desde un stage) |
//!
//! Cada una detras de su feature: un pipeline que solo corre comandos no tiene
//! por que compilar una pila de TLS.

#![warn(missing_docs)]

#[cfg(feature = "http")]
mod http;
#[cfg(feature = "postgres")]
mod pg;
#[cfg(feature = "shell")]
mod shell;
#[cfg(feature = "snowflake")]
mod snowflake;

#[cfg(feature = "http")]
pub use http::HttpTask;
#[cfg(feature = "postgres")]
pub use pg::{PostgresSink, PostgresSource};
#[cfg(feature = "shell")]
pub use shell::{ShellTask, PREFIJO_FILAS};
#[cfg(feature = "snowflake")]
pub use snowflake::SnowflakeStatement;
