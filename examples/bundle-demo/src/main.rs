//! Muchos pipelines chicos en un solo binario, generados en un bucle.
//!
//! Es la forma que toma una migracion desde Airflow cuando el proyecto genera
//! DAGs dinamicamente — por ejemplo, uno por fuente de datos. Sin bundles harian falta
//! decenas de binarios de 3 MB para ejecutar comandos de una linea.

use zeruel::prelude::*;

/// Las fuentes: en un proyecto real esto sale de leer un directorio o una
/// tabla, igual que un DAG generado dinamicamente en Airflow.
const FUENTES: &[&str] = &[
    "norte", "sur", "este", "oeste", "centro", "litoral", "cuyo", "patagonia",
];

/// Un pipeline por fuente: bajar y luego cargar.
fn para(fuente: &str) -> zeruel::Pipeline {
    let mut dag = zeruel::dag!("sync",
        "bajar" => [],
        "cargar" => ["bajar"],
    );
    dag.name = format!("sync-{fuente}");
    dag.schedule = Some("0 */6 * * *".into());

    let eco = |id: &str, texto: String| {
        if cfg!(windows) {
            ShellTask::new(id, "cmd").args(["/C", &format!("echo {texto}")])
        } else {
            ShellTask::new(id, "sh").args(["-c", &format!("echo {texto}")])
        }
    };

    zeruel::pipeline(dag)
        .register(eco("bajar", format!("bajando {fuente}")))
        .register(eco("cargar", format!("cargando {fuente}")))
        .build()
        .expect("el DAG generado tiene que ser valido")
}

fn main() -> zeruel::Result<()> {
    let mut b = zeruel::bundle();
    for f in FUENTES {
        b = b.con(para(f));
    }
    b.start()
}
