//! Tarea SQL sobre Parquet (B.2 / Fase 3): una query SQL (motor polars-sql)
//! que lee tablas Parquet, materializa el resultado y lo escribe como Parquet.
//!
//! Es una tarea de primera en el grafo: el engine la despacha igual que una
//! Rust o una Wasm (deps + param + rows + retries). Sirve para el ETL del
//! medio que no merece Polars a mano: join + filtro + agregado simple.
//!
//! Nota: polars-sql cubre el SQL tipico de ETL (SELECT/JOIN/GROUP BY/WHERE,
//! parte de window functions). NO tiene CTEs (`WITH`) ni `QUALIFY`; para eso,
//! volver a Polars a mano. Es la razon por la que no se uso DuckDB (que si
//! los tiene, a cambio de +200MB de binario y +15min de build).

use std::sync::Arc;

use anyhow::Result;
use polars::prelude::*;
use polars::sql::SQLContext;

use zeruel_sdk::{SqlSpec, Task, TaskContext, TaskId, TaskOutput};

fn pl_path(p: &std::path::Path) -> PlRefPath {
    p.to_string_lossy().as_ref().into()
}

/// Tarea que ejecuta una query SQL sobre tablas Parquet y escribe el
/// resultado a un Parquet. El `SQLContext` se construye por run (las tablas
/// se registran como `LazyFrame::scan_parquet`, asi que el Parquet se lee con
/// column pruning + predicate pushdown; no se carga todo a RAM antes de la query).
pub struct SqlTask {
    id: TaskId,
    spec: SqlSpec,
}

impl SqlTask {
    pub fn new(id: &str, spec: SqlSpec) -> Self {
        Self {
            id: TaskId(id.to_string()),
            spec,
        }
    }

    fn ejecutar(&self) -> Result<TaskOutput> {
        let mut ctx = SQLContext::new();

        // 1. registrar cada tabla como scan Parquet (lazy: pushdowns nativos)
        for (nombre, path) in &self.spec.tables {
            if !path.is_file() {
                anyhow::bail!("tabla '{}' no existe: {}", nombre, path.display());
            }
            let lf = LazyFrame::scan_parquet(pl_path(path), ScanArgsParquet::default()).map_err(
                |e| {
                    anyhow::anyhow!(
                        "no puedo leer la tabla '{}' de {}: {e}",
                        nombre,
                        path.display()
                    )
                },
            )?;
            ctx.register(nombre.as_str(), lf);
        }

        // 2. ejecutar la query
        let lf = ctx
            .execute(&self.spec.sql)
            .map_err(|e| anyhow::anyhow!("error de SQL: {e}"))?;
        let df = lf
            .collect()
            .map_err(|e| anyhow::anyhow!("falla al ejecutar la query: {e}"))?;
        let rows = df.height() as u64;

        // 3. escribir el resultado Parquet (creando el directorio si hace falta)
        let out = &self.spec.output;
        if let Some(dir) = out.parent() {
            if !dir.as_os_str().is_empty() {
                std::fs::create_dir_all(dir)
                    .map_err(|e| anyhow::anyhow!("no puedo crear {}: {e}", dir.display()))?;
            }
        }
        // row_group_size=1M permite predicate pushdown al re-leer (skipea row groups)
        let opt = ParquetWriteOptions {
            row_group_size: Some(1_000_000),
            ..Default::default()
        };
        df.lazy()
            .sink(
                SinkDestination::File {
                    target: SinkTarget::Path(pl_path(out)),
                },
                FileWriteFormat::Parquet(Arc::new(opt)),
                UnifiedSinkArgs::default(),
            )?
            .collect()
            .map_err(|e| anyhow::anyhow!("no puedo escribir {}: {e}", out.display()))?;

        Ok(TaskOutput {
            rows,
            message: format!("sql: {rows} filas -> {}", out.display()),
        })
    }
}

impl Task for SqlTask {
    fn id(&self) -> TaskId {
        self.id.clone()
    }
    fn run(&self, _ctx: &TaskContext) -> Result<TaskOutput> {
        self.ejecutar()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Arma un Parquet con 4 filas de ventas (fecha, monto, categoria).
    fn parquet_ventas(tmp: &std::path::Path) -> std::path::PathBuf {
        let p = tmp.join("ventas.parquet");
        let df = df![
            "fecha" => [1, 2, 3, 4],
            "monto" => [10.0, 20.0, 30.0, 40.0],
            "categoria" => ["a", "b", "a", "b"],
        ]
        .unwrap();
        // escribir por sink (API 0.55)
        let opt = ParquetWriteOptions::default();
        df.lazy()
            .sink(
                SinkDestination::File {
                    target: SinkTarget::Path(pl_path(&p)),
                },
                FileWriteFormat::Parquet(Arc::new(opt)),
                UnifiedSinkArgs::default(),
            )
            .unwrap()
            .collect()
            .unwrap();
        p
    }

    fn spec_ventas(tmp: &std::path::Path) -> SqlSpec {
        let tables = HashMap::from([("ventas".to_string(), parquet_ventas(tmp))]);
        SqlSpec {
            sql: "SELECT categoria, SUM(monto) AS total FROM ventas WHERE monto > 5 GROUP BY categoria ORDER BY categoria".into(),
            tables,
            output: tmp.join("out.parquet"),
        }
    }

    #[test]
    fn sql_run_escribe_parquet_con_el_resultado() {
        let tmp = tempfile::tempdir().unwrap();
        let t = SqlTask::new("q", spec_ventas(tmp.path()));
        let out = t
            .run(&TaskContext {
                run_id: uuid::Uuid::new_v4(),
                param: "t".into(),
                cancel: None,
            })
            .unwrap();

        // 2 categorias (a=40, b=60); ambas con monto > 5
        assert_eq!(out.rows, 2);
        assert!(tmp.path().join("out.parquet").is_file());

        // verificar el contenido
        let df = LazyFrame::scan_parquet(
            pl_path(&tmp.path().join("out.parquet")),
            ScanArgsParquet::default(),
        )
        .unwrap()
        .collect()
        .unwrap();
        assert_eq!(df.height(), 2);
        assert!(df.column("categoria").is_ok());
        assert!(df.column("total").is_ok());
    }

    #[test]
    fn sql_tabla_inexistente_falla_claramente() {
        let tmp = tempfile::tempdir().unwrap();
        let spec = spec_ventas(tmp.path()); // registra la tabla `ventas`
                                            // una query sobre una tabla que no registramos
        let mut spec2 = spec.clone();
        spec2.sql = "SELECT * FROM no_existe".into();
        let t = SqlTask::new("q", spec2);
        let err = t
            .run(&TaskContext {
                run_id: uuid::Uuid::new_v4(),
                param: "t".into(),
                cancel: None,
            })
            .unwrap_err();
        assert!(err.to_string().to_lowercase().contains("sql"));
    }
}
