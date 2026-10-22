//! Pipeline ETL puro (polars en Rust), sin zeruel: sin SQLite, sin metricas, sin
//! checkpoints. Solo mide el compute: read → filter → join → groupby → write.
//!
//! Es la referencia del benchmark ("cuanto de lo que tarda `zeruel run` es
//! polars y cuanto es la orquestacion"). Para que la comparacion valga, el
//! grafo y las opciones de escritura son las mismas que las del pipeline real
//! (`zeruel-data/tasks.rs`), streaming incluido.
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result};
use polars::prelude::*;

fn path(root: &str, p: &str) -> PathBuf {
    PathBuf::from(root).join(p)
}

fn pl_path(p: &Path) -> PlRefPath {
    p.to_string_lossy().as_ref().into()
}

fn read(p: &Path) -> Result<LazyFrame> {
    let lf = anyhow::Context::with_context(
        LazyFrame::scan_parquet(pl_path(p), ScanArgsParquet::default()),
        || format!("no puedo leer {}", p.display()),
    )?;
    Ok(lf.with_streaming(true))
}

/// Sinkea un LazyFrame a Parquet y devuelve las filas escritas.
fn sink_parquet(lf: LazyFrame, out: &Path) -> Result<u64> {
    if let Some(p) = out.parent() {
        std::fs::create_dir_all(p)?;
    }
    let opt = ParquetWriteOptions {
        row_group_size: Some(1_000_000),
        ..Default::default()
    };
    let r = lf
        .with_streaming(true)
        .sink(
            SinkDestination::File {
                target: SinkTarget::Path(pl_path(out)),
            },
            FileWriteFormat::Parquet(std::sync::Arc::new(opt)),
            UnifiedSinkArgs::default(),
        )?
        .collect();
    anyhow::Context::with_context(r, || format!("no puedo escribir {}", out.display()))?;
    contar(out)
}

/// Filas de un Parquet, desde los metadatos.
fn contar(p: &Path) -> Result<u64> {
    let df = read(p)?.select([len().alias("n")]).collect()?;
    Ok(df
        .column("n")
        .and_then(|c| c.get(0))
        .map(|v| v.try_extract::<u64>().unwrap_or(0))
        .unwrap_or(0))
}

fn timeit(label: &str, f: impl FnOnce() -> Result<u64>) -> Result<()> {
    let t0 = Instant::now();
    let rows = f().with_context(|| format!("etapa '{label}'"))?;
    println!(
        "{label:<20} {:>8.2}s  rows={}",
        t0.elapsed().as_secs_f64(),
        rows
    );
    Ok(())
}

pub fn run(root: &str) -> Result<()> {
    let in_dir = format!("{root}/input");
    let out_dir = format!("{root}/output_puro");
    std::fs::create_dir_all(&out_dir).with_context(|| format!("no puedo crear {out_dir}"))?;
    let total = Instant::now();

    timeit("validar", || {
        let lf = read(&path(&in_dir, "transactions.parquet"))?
            .filter(col("monto").gt(lit(0.0)).and(col("order_id").is_not_null()));
        sink_parquet(lf, &path(&out_dir, "validas.parquet"))
    })?;

    timeit("limpiar", || {
        let lf = read(&path(&out_dir, "validas.parquet"))?
            .with_column(col("categoria").str().to_lowercase());
        sink_parquet(lf, &path(&out_dir, "limpias.parquet"))
    })?;

    timeit("join_customers", || {
        let lf = read(&path(&out_dir, "limpias.parquet"))?.join(
            read(&path(&in_dir, "customers.parquet"))?,
            [col("customer_id")],
            [col("customer_id")],
            JoinArgs::new(JoinType::Inner).with_coalesce(JoinCoalesce::CoalesceColumns),
        );
        sink_parquet(lf, &path(&out_dir, "c_join.parquet"))
    })?;

    timeit("join_products", || {
        let lf = read(&path(&out_dir, "c_join.parquet"))?.join(
            read(&path(&in_dir, "products.parquet"))?,
            [col("product_id")],
            [col("product_id")],
            JoinArgs::new(JoinType::Inner).with_coalesce(JoinCoalesce::CoalesceColumns),
        );
        sink_parquet(lf, &path(&out_dir, "p_join.parquet"))
    })?;

    timeit("rollups", || {
        let lf = read(&path(&out_dir, "p_join.parquet"))?
            .with_column(col("ts").dt().date().alias("dia"))
            .group_by([col("dia"), col("categoria")])
            .agg([
                col("monto").sum().alias("total"),
                col("id").count().alias("n_trans"),
            ])
            .sort(["dia"], SortMultipleOptions::default());
        sink_parquet(lf, &path(&out_dir, "rollups.parquet"))
    })?;

    timeit("export_csv", || {
        let entrada = path(&out_dir, "rollups.parquet");
        let rows = contar(&entrada)?;
        let out = path(&out_dir, "rollups.csv");
        let r = read(&entrada)?
            .sink(
                SinkDestination::File {
                    target: SinkTarget::Path(pl_path(&out)),
                },
                FileWriteFormat::Csv(CsvWriterOptions::default()),
                UnifiedSinkArgs::default(),
            )?
            .collect();
        anyhow::Context::with_context(r, || format!("no puedo escribir {}", out.display()))?;
        Ok(rows)
    })?;

    println!("\nPURO TOTAL: {:.2}s", total.elapsed().as_secs_f64());
    Ok(())
}
