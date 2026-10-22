//! Un pipeline ETL de verdad, escrito como lo escribiria alguien que usa zeruel
//! como libreria: `cargo add zeruel`, se declara el grafo, se implementan las
//! tareas, y el binario que sale ya sabe hablar con `zeruel serve`.
//!
//! ```text
//!   bajar ──► limpiar ──► cargar
//! ```
//!
//! Correrlo suelto:
//!     cargo run -- run --param 2026-08-19 --state ./state
//!
//! Ver que le contesta al scheduler:
//!     cargo run -- --describe
//!
//! Ponerlo a correr solo: copiar el binario a `pipelines/` y levantar
//! `zeruel serve`. No hay que registrarlo en ningun lado ni recompilar nada mas.

use std::fs;
use std::path::PathBuf;

use zeruel::prelude::*;

/// Baja las ventas del dia. En un pipeline real seria un GET a una API o una
/// query a la base; aca se genera un CSV para que el ejemplo corra sin red.
struct Bajar {
    dir: PathBuf,
}

impl Task for Bajar {
    fn id(&self) -> TaskId {
        TaskId("bajar".into())
    }

    fn run(&self, ctx: &TaskContext) -> TaskResult {
        fs::create_dir_all(&self.dir)?;
        let destino = self.dir.join(format!("crudo-{}.csv", ctx.param));

        let mut csv = String::from("id,producto,monto\n");
        for i in 0..1_000 {
            // Una de cada cien viene con monto negativo: es la basura que el
            // paso siguiente tiene que filtrar.
            let monto = if i % 100 == 0 { -1.0 } else { 10.0 + (i % 50) as f64 };
            csv.push_str(&format!("{i},producto-{},{monto}\n", i % 20));
        }
        fs::write(&destino, &csv)?;

        let filas = csv.lines().count() as u64 - 1;
        Ok(TaskOutput {
            rows: filas,
            message: format!("bajadas {filas} filas a {}", destino.display()),
        })
    }
}

/// Descarta las filas invalidas (monto <= 0).
struct Limpiar {
    dir: PathBuf,
}

impl Task for Limpiar {
    fn id(&self) -> TaskId {
        TaskId("limpiar".into())
    }

    fn run(&self, ctx: &TaskContext) -> TaskResult {
        let entrada = self.dir.join(format!("crudo-{}.csv", ctx.param));
        let salida = self.dir.join(format!("limpio-{}.csv", ctx.param));

        let crudo = fs::read_to_string(&entrada)
            .map_err(|e| anyhow::anyhow!("no puedo leer {}: {e}", entrada.display()))?;

        let mut lineas = crudo.lines();
        let cabecera = lineas.next().unwrap_or("id,producto,monto");
        let mut out = String::from(cabecera);
        out.push('\n');

        let mut validas = 0u64;
        let mut descartadas = 0u64;
        for l in lineas {
            let monto: f64 = l.rsplit(',').next().and_then(|m| m.parse().ok()).unwrap_or(0.0);
            if monto > 0.0 {
                out.push_str(l);
                out.push('\n');
                validas += 1;
            } else {
                descartadas += 1;
            }
        }
        fs::write(&salida, out)?;

        Ok(TaskOutput {
            rows: validas,
            message: format!("{validas} validas, {descartadas} descartadas"),
        })
    }
}

/// Carga el resultado. En un pipeline real seria un COPY a Postgres o a
/// Snowflake; aca escribe el total por producto.
struct Cargar {
    dir: PathBuf,
}

impl Task for Cargar {
    fn id(&self) -> TaskId {
        TaskId("cargar".into())
    }

    fn run(&self, ctx: &TaskContext) -> TaskResult {
        let entrada = self.dir.join(format!("limpio-{}.csv", ctx.param));
        let limpio = fs::read_to_string(&entrada)
            .map_err(|e| anyhow::anyhow!("no puedo leer {}: {e}", entrada.display()))?;

        let mut totales: std::collections::BTreeMap<String, f64> = Default::default();
        for l in limpio.lines().skip(1) {
            let campos: Vec<&str> = l.split(',').collect();
            if campos.len() < 3 {
                continue;
            }
            let monto: f64 = campos[2].parse().unwrap_or(0.0);
            *totales.entry(campos[1].to_string()).or_default() += monto;
        }

        let salida = self.dir.join(format!("totales-{}.csv", ctx.param));
        let mut out = String::from("producto,total\n");
        for (p, t) in &totales {
            out.push_str(&format!("{p},{t:.2}\n"));
        }
        fs::write(&salida, out)?;

        Ok(TaskOutput {
            rows: totales.len() as u64,
            message: format!("{} productos cargados", totales.len()),
        })
    }
}

fn main() -> zeruel::Result<()> {
    let dir = PathBuf::from("datos");

    let mut dag = zeruel::dag!("ventas-diarias",
        "bajar"   => [],
        "limpiar" => ["bajar"],
        "cargar"  => ["limpiar"],
    );
    // Todos los dias a las 6 UTC. `zeruel serve` lo dispara solo; sin esto, el
    // pipeline queda disponible solo bajo demanda.
    dag.schedule = Some("0 6 * * *".into());

    zeruel::pipeline(dag)
        .register(Bajar { dir: dir.clone() })
        .register(Limpiar { dir: dir.clone() })
        .register(Cargar { dir })
        .start()
}
