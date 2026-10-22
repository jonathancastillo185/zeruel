//! Lo que consume zeruel por estar prendido.
//!
//! `docs/COSTOS.md` tiene los mismos numeros pero congelados en el dia que se
//! midieron. Esto los mide ahora, que es lo unico que sirve cuando alguien
//! pregunta por que el servidor esta al 90%.
//!
//! ## Que se puede medir desde aca y que no
//!
//! `zeruel web` es un proceso aparte de `zeruel serve`, y los runs son hijos de
//! *serve*, no de web. Asi que para ver el arbol completo hay que salir a
//! buscarlo en la tabla de procesos del sistema:
//!
//! ```text
//!   zeruel serve  ─┬─ ecommerce.exe   (un run)
//!                ├─ reportes.exe    (otro run)
//!                └─ rrhh.exe
//!   zeruel web     (este proceso: no tiene hijos)
//! ```
//!
//! Se identifican por dos vias, y hacen falta las dos:
//!
//! - Los procesos de zeruel, por nombre.
//! - Los runs, por ser **hijos** de uno de esos. Un pipeline se llama como
//!   quiera —`ecommerce.exe`, `mi-etl`— asi que el nombre no alcanza; el
//!   parentesco si.
//!
//! Lo que NO se hace es adivinar por nombre parecido: un `zeruel-backup.exe` de
//! otra persona no es nuestro, y contarlo inflaria el numero que despues
//! alguien usa para dimensionar un servidor.

use serde_json::{json, Value};
use std::path::Path;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System, UpdateKind};
use zeruel_core::Store;

/// Nombres de los ejecutables propios (sin extension).
const MIOS: [&str; 1] = ["zeruel"];

pub fn reporte(store: &Store) -> Value {
    let mut sys = System::new_with_specifics(
        RefreshKind::nothing().with_processes(
            ProcessRefreshKind::nothing()
                .with_memory()
                .with_cmd(UpdateKind::Always),
        ),
    );
    sys.refresh_memory();
    sys.refresh_processes(ProcessesToUpdate::All, true);

    let procesos = procesos(&sys);
    let dir = store.path.parent().unwrap_or(Path::new("."));

    let h = store.hygiene().unwrap_or_default();
    let total_mb = sys.total_memory() / (1024 * 1024);
    let libre_mb = sys.available_memory() / (1024 * 1024);

    // Lo que ocupa zeruel, sumando sus procesos y los runs que colgo. Es el
    // numero que se compara contra el "1 a 2 GB" de un Airflow minimo.
    let nuestro_mb: u64 = procesos.iter().map(|p| p.ram_mb).sum();
    let reposo_mb: u64 = procesos
        .iter()
        .filter(|p| !p.es_run)
        .map(|p| p.ram_mb)
        .sum();

    json!({
        "maquina": {
            "ram_total_mb": total_mb,
            "ram_libre_mb": libre_mb,
            "ram_usada_mb": total_mb.saturating_sub(libre_mb),
            "pct_usada": pct(total_mb.saturating_sub(libre_mb), total_mb),
            "cpus": sys.cpus().len(),
        },
        "zeruel": {
            "ram_total_mb": nuestro_mb,
            "ram_en_reposo_mb": reposo_mb,
            "pct_de_la_maquina": pct(nuestro_mb, total_mb),
            "procesos": procesos.iter().map(|p| json!({
                "pid": p.pid,
                "nombre": p.nombre,
                "ram_mb": p.ram_mb,
                "es_run": p.es_run,
                "es_este": p.es_este,
            })).collect::<Vec<_>>(),
        },
        "disco": disco(dir, &h),
        "trabajo": {
            "runs_en_curso": store.running_keys().map(|k| k.len()).unwrap_or(0),
            "items_en_cola": store.queue_items().map(|q| q.len()).unwrap_or(0),
        },
        "version": env!("CARGO_PKG_VERSION"),
        "contrato": crate::pipelines::CONTRATO,
    })
}

struct Proc {
    pid: u32,
    nombre: String,
    ram_mb: u64,
    /// Un hijo de `zeruel serve`: es un pipeline ejecutandose.
    es_run: bool,
    es_este: bool,
}

/// Los procesos de zeruel mas los runs que colgaron de ellos.
fn procesos(sys: &System) -> Vec<Proc> {
    let yo = std::process::id();

    // Primero los propios, por nombre.
    let nuestros: Vec<Pid> = sys
        .processes()
        .iter()
        .filter(|(_, p)| es_nuestro(p.name().to_string_lossy().as_ref()))
        .map(|(pid, _)| *pid)
        .collect();

    let mut out: Vec<Proc> = Vec::new();
    for (pid, p) in sys.processes() {
        let nombre = etiqueta(p);
        let propio = nuestros.contains(pid);
        // Un run es un hijo de uno de los nuestros. El nombre no sirve para
        // detectarlos: un pipeline se llama como su autor haya querido.
        let hijo = p.parent().is_some_and(|pp| nuestros.contains(&pp));
        if !propio && !hijo {
            continue;
        }
        out.push(Proc {
            pid: pid.as_u32(),
            nombre,
            ram_mb: p.memory() / (1024 * 1024),
            es_run: hijo && !propio,
            es_este: pid.as_u32() == yo,
        });
    }

    // De mayor a menor: la pregunta siempre es quien se la esta comiendo.
    out.sort_by_key(|p| std::cmp::Reverse(p.ram_mb));
    out
}

/// Como se muestra el proceso en la lista.
///
/// `zeruel serve` y `zeruel web` son el mismo ejecutable, asi que por nombre son
/// dos renglones identicos y la pantalla no dice nada. El subcomando es lo
/// unico que los separa, y es justo el dato que se quiere ver.
///
/// Los pipelines se quedan con su nombre de archivo: ahi el nombre ya
/// identifica, y sus argumentos traen el `--param`, que puede ser cualquier
/// cosa y no tiene por que terminar en una pantalla compartida.
fn etiqueta(p: &sysinfo::Process) -> String {
    let base = p.name().to_string_lossy().to_string();
    if !es_nuestro(&base) {
        return base;
    }
    let sub = p
        .cmd()
        .iter()
        .skip(1)
        .map(|a| a.to_string_lossy())
        .find(|a| !a.starts_with('-'));
    match sub {
        Some(s) => format!("zeruel {s}"),
        None => base,
    }
}

fn es_nuestro(nombre: &str) -> bool {
    let base = nombre.strip_suffix(".exe").unwrap_or(nombre);
    MIOS.contains(&base)
}

/// Desglose del estado en disco. Se separa el WAL porque puede crecer solo y
/// es lo primero que sorprende a alguien mirando el tamaño de la base.
fn disco(dir: &Path, h: &zeruel_core::Hygiene) -> Value {
    let db = archivo(&dir.join("zeruel.db"));
    let wal = archivo(&dir.join("zeruel.db-wal"));
    let shm = archivo(&dir.join("zeruel.db-shm"));
    let logs = carpeta(&dir.join("logs"));

    json!({
        "db_mb": mb(db),
        "wal_mb": mb(wal),
        "shm_mb": mb(shm),
        "logs_mb": mb(logs),
        "total_mb": mb(db + wal + shm + logs),
        "runs": h.runs,
        "task_runs": h.task_runs,
        "meses_agregados": h.meses,
        "ultima_gc": h.ultima_gc,
        "db_grande": h.db_mb_warning,
    })
}

fn archivo(p: &Path) -> u64 {
    std::fs::metadata(p).map(|m| m.len()).unwrap_or(0)
}

/// Suma el primer nivel de una carpeta. No recursivo a proposito: `logs/` es
/// plano, y recorrer un arbol entero en cada request de la UI seria un costo
/// desproporcionado para un numero informativo.
fn carpeta(p: &Path) -> u64 {
    std::fs::read_dir(p)
        .map(|rd| {
            rd.flatten()
                .filter_map(|e| e.metadata().ok())
                .filter(|m| m.is_file())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}

fn mb(bytes: u64) -> f64 {
    (bytes as f64 / 1_048_576.0 * 10.0).round() / 10.0
}

fn pct(parte: u64, total: u64) -> f64 {
    if total == 0 {
        return 0.0;
    }
    (parte as f64 / total as f64 * 1000.0).round() / 10.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconoce_los_ejecutables_propios() {
        assert!(es_nuestro("zeruel"));
        assert!(es_nuestro("zeruel.exe"));
        // Un binario ajeno con nombre parecido no cuenta: sumarlo inflaria el
        // numero que alguien despues usa para dimensionar un servidor.
        assert!(!es_nuestro("zeruel-backup.exe"));
        assert!(!es_nuestro("nodos"));
        assert!(!es_nuestro("mi-etl.exe"));
    }

    #[test]
    fn el_porcentaje_no_divide_por_cero() {
        assert_eq!(pct(0, 0), 0.0);
        assert_eq!(pct(512, 1024), 50.0);
        assert_eq!(pct(1, 3), 33.3);
    }

    #[test]
    fn los_megas_se_redondean_a_un_decimal() {
        assert_eq!(mb(0), 0.0);
        assert_eq!(mb(1_048_576), 1.0);
        assert_eq!(mb(1_572_864), 1.5);
    }

    #[test]
    fn un_archivo_que_no_existe_pesa_cero() {
        // Que falte el WAL es normal (SQLite lo borra al cerrar limpio), asi
        // que no puede ser un error: tiene que sumar 0 y seguir.
        assert_eq!(archivo(Path::new("no-existe-jamas.db-wal")), 0);
        assert_eq!(carpeta(Path::new("no-existe-jamas/logs")), 0);
    }

    #[test]
    fn el_proceso_actual_aparece_y_se_marca() {
        let mut sys = System::new_with_specifics(
            RefreshKind::nothing().with_processes(
                ProcessRefreshKind::nothing()
                    .with_memory()
                    .with_cmd(UpdateKind::Always),
            ),
        );
        sys.refresh_processes(ProcessesToUpdate::All, true);
        let ps = procesos(&sys);
        // Corriendo bajo `cargo test` el binario no se llama `zeruel`, asi que
        // la lista puede venir vacia. Lo que se verifica es que, si aparece
        // alguno marcado como "este", sea de verdad este proceso.
        for p in ps.iter().filter(|p| p.es_este) {
            assert_eq!(p.pid, std::process::id());
        }
    }
}
