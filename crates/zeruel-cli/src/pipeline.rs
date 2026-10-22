//! Pipeline de ejemplo (Hito 1): transacciones de e-commerce.
//!
//! Grafo (lineal):
//!   validar -> limpiar -> join_cust -> join_prod -> rollups -> export
//!
//! B.1/B.2: se pueden sumar tareas WASM y SQL opt-in (via `zeruel run --wasm` /
//! `--sql-file`) como tareas mas del grafo, sin alterar el invariante de filas
//! del benchmark (solo existen cuando el usuario las pide).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use polars::prelude::*;
use zeruel_core::Engine;
use zeruel_data::{DataEnv, ExportCsvTask, JoinTask, PolarsTask};
use zeruel_sdk::{Dag, SqlSpec, Task, TaskDef, TaskId, WasmSpec};

pub struct Pipeline {
    pub dag: Dag,
    pub tasks: HashMap<TaskId, Arc<dyn Task>>,
}

/// Schedule cron declarado por el DAG de ejemplo (None = solo bajo demanda).
/// [deprecated] ahora usa `dags::dag_cron`. Se deja para compat.
#[allow(dead_code)]
pub fn dag_cron(env: &DataEnv) -> Option<(String, String)> {
    let p = build(env, None, None);
    p.dag
        .schedule
        .as_ref()
        .map(|s| (p.dag.name.clone(), s.clone()))
}

/// Build del DAG de ejemplo (sin opt-ins). Lo usa la CLI `zeruel run` (agregando
/// opt-in wasm/sql) y el registry multi-DAG (`dags.rs`).
pub fn build_ecommerce(env: &DataEnv) -> Pipeline {
    build(env, None, None)
}

pub fn build(env: &DataEnv, wasm: Option<&WasmSpec>, sql: Option<&SqlSpec>) -> Pipeline {
    let mut dag = zeruel_sdk::dag!("ecommerce-etl",
        "validar"        => [],
        "limpiar"        => ["validar"],
        "join_customers" => ["limpiar"],
        "join_products"  => ["join_customers"],
        "rollups"        => ["join_products"],
        "export_csv"     => ["rollups"],
    );
    // Schedule cron diario (06:00 UTC). `zeruel serve` lo dispara solo si llega ese
    // minuto; si no, el DAG sigue disponible bajo demanda (`zeruel run`/`enqueue`).
    dag.schedule = Some("0 6 * * *".to_string());
    let mut tasks: HashMap<TaskId, Arc<dyn Task>> = HashMap::new();

    // 1. validar: monto>0 y order_id no nulo
    let validar = Arc::new(PolarsTask::new(
        env.clone(),
        "validar",
        "input/transactions.parquet",
        "output/validas.parquet",
        |lf| Ok(lf.filter(col("monto").gt(lit(0.0)).and(col("order_id").is_not_null()))),
    )) as Arc<dyn Task>;

    // 2. limpiar: normalizar categoria (minusculas, espacios a guion)
    let limpiar = Arc::new(PolarsTask::new(
        env.clone(),
        "limpiar",
        "output/validas.parquet",
        "output/limpias.parquet",
        |lf| Ok(lf.with_column(col("categoria").str().to_lowercase().alias("categoria"))),
    )) as Arc<dyn Task>;

    // 3. join con customers
    let join_cust = Arc::new(JoinTask::new(
        env.clone(),
        "join_customers",
        "output/limpias.parquet",
        "input/customers.parquet",
        "customer_id",
        "output/con_cliente.parquet",
    )) as Arc<dyn Task>;

    // 4. join con products
    let join_prod = Arc::new(JoinTask::new(
        env.clone(),
        "join_products",
        "output/con_cliente.parquet",
        "input/products.parquet",
        "product_id",
        "output/enriquecida.parquet",
    )) as Arc<dyn Task>;

    // 5. rollups por dia y categoria
    let rollups = Arc::new(PolarsTask::new(
        env.clone(),
        "rollups",
        "output/enriquecida.parquet",
        "output/rollups.parquet",
        |lf| {
            Ok(lf
                .with_column(col("ts").dt().date().alias("dia"))
                .group_by([col("dia"), col("categoria")])
                .agg([
                    col("monto").sum().alias("total"),
                    col("id").count().alias("n_trans"),
                ])
                .sort(["dia"], SortMultipleOptions::default()))
        },
    )) as Arc<dyn Task>;

    // 6. export CSV del rollup
    let export = Arc::new(ExportCsvTask::new(
        env.clone(),
        "export_csv",
        "output/rollups.parquet",
        "output/rollups.csv",
    )) as Arc<dyn Task>;

    for t in [validar, limpiar, join_cust, join_prod, rollups, export] {
        tasks.insert(t.id(), t);
    }

    // B.1: tarea WASM opt-in. Depende de `validar` (raiz) para demostrar el
    // dispatch mixto rust+wasm con deps.
    if let Some(spec) = wasm {
        dag.tasks.push(TaskDef::wasm(
            "wasm_etl",
            vec![TaskId("validar".into())],
            spec.clone(),
        ));
    }

    // B.2: tarea SQL opt-in. Depende de `export_csv` (la ultima tarea del
    // pipeline) para que todos los Parquet que pueda leer ya esten producidos:
    // si dependiera de una tarea que escribe un input de la query, la query lo
    // leeria en paralelo. El dispatch es el mismo que el resto (deps/param/rows/retries).
    if let Some(spec) = sql {
        dag.tasks.push(TaskDef::sql(
            "sql_etl",
            vec![TaskId("export_csv".into())],
            spec.clone(),
        ));
    }

    Pipeline { dag, tasks }
}

pub fn run(
    env: &DataEnv,
    store: &Arc<zeruel_core::Store>,
    max_parallel: usize,
    param: &str,
    wasm: Option<&WasmSpec>,
    sql: Option<&SqlSpec>,
) -> anyhow::Result<zeruel_core::RunReport> {
    let p = build(env, wasm, sql);
    let engine = Engine::new(store.clone());
    engine.run(&p.dag, &p.tasks, param, max_parallel)
}

/// Parsea la spec de tablas de `zeruel run --sql-tables`:
/// `nombre=ruta,otra=ruta2`. Devuelve el mapeo nombre -> PathBuf (relativo o
/// absoluto; el caller lo resuelve contra el base con `DataEnv::path`).
///
/// Las rutas NO pueden contener `,` (separador de pares). Es un formato simple
/// para la CLI; para tablas con rutas exoticas se usa el builder Rust.
pub fn parse_sql_tables(spec: &str) -> anyhow::Result<Vec<(String, PathBuf)>> {
    let mut out = Vec::new();
    if spec.trim().is_empty() {
        return Ok(out);
    }
    for par in spec.split(',') {
        let par = par.trim();
        if par.is_empty() {
            continue;
        }
        let (nombre, ruta) = par
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("--sql-tables: par sin '=': {par:?}"))?;
        let nombre = nombre.trim();
        let ruta = ruta.trim();
        if nombre.is_empty() || ruta.is_empty() {
            anyhow::bail!("--sql-tables: nombre o ruta vacios en {par:?}");
        }
        out.push((nombre.to_string(), PathBuf::from(ruta)));
    }
    Ok(out)
}
